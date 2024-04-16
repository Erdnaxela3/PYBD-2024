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

    :param df: pd.DataFrame
    :return: pd.DataFrame
    """
    stocks["volume_diff"] = stocks.groupby(
        [stocks.index.get_level_values("symbol"), stocks.index.get_level_values(0).date]
    )["volume"].diff()
    stocks.fillna({"volume_diff": stocks.volume}, inplace=True)


def remove_negative_volume(stocks: pd.DataFrame):
    """
    Compute volume_diff and remove negative values.
    Volume MUST NOT be negative.

    :param df: pd.DataFrame
    :return: pd.DataFrame
    """
    compute_volume_diff(stocks)

    nb_bad_values = len(stocks.loc[stocks.volume_diff < 0])
    while nb_bad_values != 0:
        compute_volume_diff(stocks)
        stocks = stocks[stocks["volume_diff"] >= 0]

        nb_bad_values = len(stocks.loc[stocks.volume_diff < 0])


def compute_daystocks(stocks: pd.DataFrame) -> pd.DataFrame:
    """
    Compute a dataframe with (date, cid, open, close, high, low, volume, mean, std) for each day.

    :param df: pd.DataFrame with indexes:'date', 'cid' and columns: value', 'volume'
    :return: pd.DataFrame with 'date', 'cid', 'open', 'close', 'high', 'low', 'volume', 'mean', 'std'
    """
    grouped = stocks.groupby([stocks.index.get_level_values("cid"), stocks.index.get_level_values(0).date])
    daystocks = grouped["value"].ohlc()
    daystocks.dropna(inplace=True)
    daystocks.index.rename("date", level=1, inplace=True)

    # TODO compute volume, sum for the day
    # TODO compute mean
    # TODO compute std

    return daystocks


def load_df_from_files(files: list[str]) -> pd.DataFrame:
    """
    Load a dataframe from a list of files.

    :param files: list[str]
    :return: pd.DataFrame
    """
    df_dict = {}
    for file in files:
        if not db.is_file_done(file)[0][0]:
            date = dateutil.parser.parse(".".join(" ".join(file.split()[1:]).split(".")[:-1]))
            df_dict[date] = pd.read_pickle(file)
        else:
            files.remove(file)

    if df_dict == {}:
        return None

    df = pd.concat(df_dict)
    df.sort_index(inplace=True)  # chronological order

    return df

def process_stocks(unprocessed_stocks: pd.DataFrame):
    """
    Turn a unprocessed_stocks dataframe into a stocks dataframe.
    With symbol, without cid for now.

    Rename column 'last' to 'value'.
    Floatify 'value'.
    Compute volume_diff and remove negative values.
    'volume_diff' replaces 'volume' and gets renamed to 'volume'.

    The unprocessed df becomes: date, symbol, (last renamed to) value, volume, name

    :param unprocessed_stocks: pd.DataFrame
    """
    unprocessed_stocks.rename(columns={"last": "value"}, inplace=True)
    unprocessed_stocks["value"] = unprocessed_stocks["value"].apply(floatify).astype(float)
    remove_negative_volume(unprocessed_stocks)
    unprocessed_stocks.drop(columns=["volume"], inplace=True)
    unprocessed_stocks.rename(columns={"volume_diff": "volume"}, inplace=True)
    unprocessed_stocks["volume"] = unprocessed_stocks["volume"].astype(int)

# TODO optimize this (maybe using db.__boursorama_cid dict to avoid db queries)
# TODO handle companies that have changed name or/and symbol
# TODO handle NV, T... (to discuss: maybe remove the stocks with low std or with very little data)
def process_companies(stocks: pd.DataFrame):
    """
    Create new entries in companies table for each (name, symbol, market id) in stocks, if not already in db.
    Replace (name, symbol) by cid in stocks.

    :param stocks: pd.DataFrame (date, symbol, value, volume, symbol, name)
    :param market: str the market the companies will belong to (amsterdam, compA, compB...)
    """
    name_symbol = stocks[['name', 'symbol']].drop_duplicates().values
    logger.log(mylogging.DEBUG, f"Found {len(name_symbol)} companies.")

    n = len(name_symbol)
    progess = 0

    for name, symbol in name_symbol:
        progess += 1
        logger.log(mylogging.DEBUG, f"Processing company {progess}/{n}.")

        id = db.search_company_id_from_symbol(symbol)
        logger.log(mylogging.DEBUG, f"Found company {name} {symbol} with id {id}.")
        if id == 0:
            db.execute(
                "INSERT INTO companies (name, symbol) VALUES (%s, %s)",
                (
                    name,
                    symbol,
                ),
            )
            id = db.search_company_id_from_symbol(symbol)
            logger.log(mylogging.DEBUG, f"Inserted company {name} {symbol} with id {id}.")
        stocks.loc[stocks['symbol'] == symbol, 'cid'] = id
        stocks.loc[stocks['symbol'] == symbol, 'name'] = name

    stocks.set_index("cid", append=True, inplace=True)
    stocks.reset_index(level=1, drop=True, inplace=True)
    stocks.drop(columns=['name','symbol'], inplace=True)
    stocks.index.rename("date", level=0, inplace=True)

    logger.log(mylogging.DEBUG, f"\n{stocks.head()}")

# TODO : store_month instead of year, year takes too much RAM (16GB+)
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
    :param market: str
    :return: list[str] list of files stored
    """
    files = glob.glob(f"{BOURSORAMA_PATH}/{year}/* {year}-{month}-0*") # TODO : remove -0 to do all days of the month and not only the first 9
    logger.log(mylogging.INFO, f"Storing {year} {month}. Found {len(files)} files.")

    if len(files) == 0:
        return []

    logger.log(mylogging.INFO, f"Loading {year} {month}.")
    stocks = load_df_from_files(files)

    if stocks is None:
        return []

    process_stocks(stocks)

    logger.log(mylogging.INFO, f"Adding companies {year} {month}.")
    process_companies(stocks)
    db.df_write(stocks, 'stocks', commit=True)

    logger.log(mylogging.INFO, f"Computing daystock {year} {month}.")
    daystocks = compute_daystocks(stocks)

    db.df_write(daystocks, 'daystocks', commit=True)

    return files


if __name__ == "__main__":
    years = os.listdir(BOURSORAMA_PATH)
    months = [f"{month:02d}" for month in range(1, 2)] # TODO: change 2 to 13 to do all months

    file_count = 0

    for year in years:
        for month in months:
            files = store_month(year, month)
            file_count += len(files)

            for file in files:
                db.execute("INSERT INTO file_done (name) VALUES (%s)", (file,), commit=True)
            logger.log(mylogging.DEBUG, f'file_done count: {db.execute("SELECT COUNT(name) FROM file_done")[0][0]}')

    logger.log(mylogging.DEBUG, f"Stored {file_count} files in total (should be 271325).")
