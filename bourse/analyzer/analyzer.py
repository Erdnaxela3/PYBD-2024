import pandas as pd
import numpy as np
import sklearn
import re
import glob
import dateutil
import os
import mylogging

import timescaledb_model as tsdb

logger = mylogging.getLogger("analyzer", level=mylogging.DEBUG)

db = tsdb.TimescaleStockMarketModel("bourse", "ricou", "db", "monmdp")  # inside docker
# db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'localhost', 'monmdp') # outside docker

BOURSORAMA_PATH = "data/boursorama"


def floatify(x) -> float:
    """
    Convert anything (str|float|int) to a float, removing spaces if necessary.
    Defined floatify(x) -> float because .str.replace(' ', '').astype(float) + pd.to_numeric worked only on strings.
    Doing the operation on a float would result in a NaN.

    Handle:
    - regular numeric (13, 0.14, 2.343)
    - string ('13.0', '1321.491823', '12  222.222', '34.23 (c)')

    :param x: str|float|int
    :return: float
    """
    try:
        return float(re.sub(r"[^0-9.]", "", x))
    except:
        return x


def compute_volume_diff(stocks: pd.DataFrame):
    """
    Compute the actual volume instead of cumulative volume intra-day (volume column).
    Create a new column volume_diff.
    The first volume_diff of a day is the same as its volume.

    Resulting in (date, symbol, value, volume, volume_diff, name).

    :param stocks: pd.DataFrame (date, symbol, value, volume, name)
    """
    stocks["volume_diff"] = stocks.groupby(
        [stocks.index.get_level_values("symbol"), stocks.index.get_level_values("date").date]
    )["volume"].diff()
    stocks.fillna({"volume_diff": stocks.volume}, inplace=True)


def remove_negative_volume(stocks: pd.DataFrame):
    """
    Compute volume_diff and remove negative values.
    Volume MUST NOT be negative.

    :param stocks: pd.DataFrame (date, symbol, value, volume, volume_diff, name)
    """
    compute_volume_diff(stocks)

    nb_bad_values = len(stocks.loc[stocks.volume_diff < 0])
    while nb_bad_values != 0:
        compute_volume_diff(stocks)
        stocks.drop(stocks[stocks["volume_diff"] < 0].index, inplace=True)
        nb_bad_values = len(stocks.loc[stocks.volume_diff < 0])


def compute_daystocks(stocks: pd.DataFrame) -> pd.DataFrame:
    """
    Compute a dataframe with (date, cid, open, close, high, low, volume, mean, std) for each day.

    Removes rows with volume exceeding the MAX value for INT in postgres (4 bytes int) as they can not be stored.

    :param stocks: pd.DataFrame with indexes: ('date', 'cid') and columns: (value', 'volume')
    :return: pd.DataFrame with ('date', 'cid'), 'open', 'close', 'high', 'low', 'volume', 'mean', 'std'
    """
    grouped = stocks.groupby([stocks.index.get_level_values("cid"), stocks.index.get_level_values("date").date])
    daystocks = grouped["value"].ohlc()

    # second index date loses its name, need to reset it
    daystocks.index.rename("date", level=1, inplace=True)

    daystocks["mean"] = grouped['value'].mean()
    daystocks["std"] = grouped["value"].std()
    daystocks['volume'] = grouped["volume"].sum()

    max_int_value = 2 ** 31 - 1  # 4 bytes int
    daystocks.drop(daystocks[daystocks["volume"] >= max_int_value].index, inplace=True)

    # log max volume
    max_volume = daystocks['volume'].max()
    logger.log(mylogging.DEBUG, f"Max volume: {max_volume}")

    return daystocks


def load_df_from_files(files: list[str]) -> pd.DataFrame|None:
    """
    Load a dataframe from a list of files.
    Sort by date.
    Name level_0 index to date.
    Remove duplicate 'symbol' columns (because both in index and columns).
    Rename 'last' to 'value'.

    :param files: list[str] list of files to load
    :return: pd.DataFrame index: (date, symbol), columns: value, volume, name
    """
    df_dict = {}
    for file in files:
        if not db.is_file_done(file)[0][0]:
            date = dateutil.parser.parse(".".join(" ".join(file.split()[1:]).split(".")[:-1]))
            if date in df_dict:
                df_dict[date] = pd.concat([df_dict[date], pd.read_pickle(file)])
            else:
                df_dict[date] = pd.read_pickle(file)
        else:
            files.remove(file)

    if df_dict == {}:
        return None

    df = pd.concat(df_dict)

    df.sort_index(inplace=True)
    df.index.rename("date", level=0, inplace=True)
    df.drop(columns=["symbol"], inplace=True)
    df.rename(columns={"last": "value"}, inplace=True)

    return df


def process_stocks(unprocessed_stocks: pd.DataFrame):
    """
    Turns an unprocessed_stocks dataframe (date, symbol, value, volume, name)
    into a stocks dataframe (date, symbol, value, volume, name).

    Floatify 'value'.
    Take mean of 'value' if multiple values are given for a same timestamp.
    Compute volume_diff and remove negative values.
    Remove volume_diff exceeding the MAX value for INT in postgres (4 bytes int).
    'volume_diff' replaces 'volume' and gets renamed to 'volume'.

    :param unprocessed_stocks: pd.DataFrame (date, symbol, value, volume, name)
    """
    unprocessed_stocks["value"] = unprocessed_stocks["value"].apply(floatify).astype(float)

    # TODO handle NV, T... (to discuss: maybe remove the stocks with low std or with very little data)

    df_len = len(unprocessed_stocks)
    unprocessed_stocks['value'] = unprocessed_stocks.groupby(['date', 'symbol'])['value'].mean()
    unprocessed_stocks['volume'] = unprocessed_stocks.groupby(['date', 'symbol'])['volume'].mean()
    unprocessed_stocks.reset_index(inplace=True)
    unprocessed_stocks.drop_duplicates(inplace=True)
    unprocessed_stocks.set_index(["date", "symbol"], inplace=True)

    logger.log(mylogging.DEBUG, f"Averaging {df_len - len(unprocessed_stocks)} common datapoint from different market.")

    df_len = len(unprocessed_stocks)
    max_int_value = 2 ** 31 - 1  # 4 bytes int
    remove_negative_volume(unprocessed_stocks)
    unprocessed_stocks.drop(columns=["volume"], inplace=True)

    removed_rows = df_len - len(unprocessed_stocks)
    percentage_removed = removed_rows / df_len * 100
    logger.log(mylogging.DEBUG, f"Removed {removed_rows} ({percentage_removed:.2f}%) bad data (negative volume).")

    df_len = len(unprocessed_stocks)
    unprocessed_stocks.drop(unprocessed_stocks[unprocessed_stocks["volume_diff"] >= max_int_value].index, inplace=True)
    unprocessed_stocks.drop(unprocessed_stocks[unprocessed_stocks["value"] >= max_int_value].index, inplace=True)

    removed_rows = df_len - len(unprocessed_stocks)
    percentage_removed = removed_rows / df_len * 100
    logger.log(mylogging.DEBUG,
               f"Removed {removed_rows} ({percentage_removed:.2f}%) bad data (too big volume or value).")

    unprocessed_stocks.rename(columns={"volume_diff": "volume"}, inplace=True)
    unprocessed_stocks["volume"] = unprocessed_stocks["volume"].astype(int)


def process_companies(stocks: pd.DataFrame):
    """
    Create new entries in companies table for each (name, symbol) in stocks, if not already in db.
    Replace (name, symbol) by cid in stocks.

    Resulting in (date, cid, value, volume).

    :param stocks: pd.DataFrame (date, symbol, value, volume, name)
    """
    new_companies_df = stocks[['name']].drop_duplicates()
    new_companies_df.reset_index(inplace=True)
    new_companies_df.drop(columns=['date'], inplace=True)

    companies_df = db.df_query("SELECT id, name, symbol FROM companies", chunksize=None)

    merge_df = new_companies_df.merge(companies_df, on=['name', 'symbol'], how='left', indicator=True)
    new_companies_df = merge_df[merge_df['_merge'] == 'left_only'][['name', 'symbol']]

    logger.log(mylogging.DEBUG, f"New companies to add/update: {len(new_companies_df)}")

    for name, symbol in new_companies_df.values:
        id = companies_df[companies_df['symbol'] == symbol]['id']
        if len(id) != 0:
            old_name = companies_df[companies_df['symbol'] == symbol]['name'].values[0]
            if old_name != name and not name.startswith("SRD"):
                db.execute("UPDATE companies SET name = %s WHERE id = %s", (name, float(id.values[0])), commit=True)

                new_companies_df.drop(
                    new_companies_df[(new_companies_df['name'] == name) & (new_companies_df['symbol'] == symbol)].index,
                    inplace=True)

                # logger.log(mylogging.DEBUG,
                #            f"Updated company from {old_name} to {name} {symbol}, new len {len(new_companies_df)}")

    new_companies_df.reset_index(drop=True, inplace=True)

    db.df_write(new_companies_df, 'companies', index=False, commit=True)
    companies_df = db.df_query("SELECT id, symbol FROM companies", chunksize=None)

    stocks.drop(columns=['name'], inplace=True)
    stocks.reset_index(inplace=True)  # stocks column date, value, volume, symbol

    stocks = stocks.merge(companies_df, left_on='symbol', right_on='symbol')  # date, value, volume, symbol, id
    stocks.rename(columns={'id': 'cid'}, inplace=True)

    stocks.set_index(["date", "cid"], inplace=True)
    stocks.drop(columns=['symbol'], inplace=True)

    return stocks


def store_month(year: str, month: str) -> list[str]:
    """
    Store a month of data on the database.
    Store in the database the stocks, companies and daystocks.

    Load data: date, symbol, last, volume, name
    Process data: floatify last, compute volume_diff, remove negative volume

    pre-stocks: date, symbol, value, volume, name

    companies(from pre-stocks): cid, name, symbol
    stocks (from pre-stocks): date, cid, value, volume
    daystocks (from stocks): date, cid, open, close, high, low, volume, mean, std

    :param year: str
    :param month: str
    :return: list[str] list of files stored
    """
    files = glob.glob(f"{BOURSORAMA_PATH}/{year}/* {year}-{month}*")
    logger.log(mylogging.INFO, f"Storing {year} {month}. Found {len(files)} files.")

    if len(files) == 0:
        return []

    logger.log(mylogging.INFO, f"Loading {year} {month}.")
    stocks = load_df_from_files(files)

    if stocks is None:
        return []

    process_stocks(stocks)

    logger.log(mylogging.INFO, f"Adding companies {year} {month}.")
    stocks = process_companies(stocks)

    logger.log(mylogging.INFO, f"Storing stocks {year} {month} in DB.")
    db.df_write(stocks, 'stocks', commit=True)

    logger.log(mylogging.INFO, f"Computing daystock {year} {month}.")
    daystocks = compute_daystocks(stocks)

    logger.log(mylogging.INFO, f"Storing daystocks {year} {month} in DB.")
    db.df_write(daystocks, 'daystocks', commit=True)

    return files


if __name__ == "__main__":
    years = [str(year) for year in range(2019, 2024)]
    months = [f"{month:02d}" for month in range(1, 13)]

    file_count = 0

    for year in years:
        for month in months:
            files = store_month(year, month)
            file_count += len(files)

            for file in files:
                db.execute("INSERT INTO file_done (name) VALUES (%s)", (file,), commit=True)
            logger.log(mylogging.DEBUG, f'file_done count: {db.execute("SELECT COUNT(name) FROM file_done")[0][0]}')

    logger.log(mylogging.DEBUG, f"Stored {file_count} files in total (should be 271325).")
