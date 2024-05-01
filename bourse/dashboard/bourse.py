import dash
from dash import dcc
from dash import html
import dash.dependencies as ddep
import pandas as pd
import sqlalchemy
import plotly.graph_objects as go
from datetime import date

from dash.dcc import RadioItems

# external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

DATABASE_URI = "timescaledb://ricou:monmdp@db:5432/bourse"  # inside docker
# DATABASE_URI = 'timescaledb://ricou:monmdp@localhost:5432/bourse'  # outisde docker
engine = sqlalchemy.create_engine(DATABASE_URI)

app = dash.Dash(__name__, title="Bourse",
                suppress_callback_exceptions=True)  # , external_stylesheets=external_stylesheets)
server = app.server


def get_companies() -> pd.DataFrame:
    """
    Get companies id, name and symbol from the database.

    :return: pd.DataFrame
    """
    query = f"SELECT id, name, symbol FROM companies"
    companies_df = pd.read_sql(query, engine)
    return companies_df


def companies_dropdown() -> dcc.Dropdown:
    """
    Dropdown to select companies to display.

    :return: dcc.Dropdown
    """
    companies_df = get_companies()
    return dcc.Dropdown(
        id="companies-dropdown",
        options=[
            {
                "label": f"{row['name']} ({row['symbol']})",
                # TODO find a better way to keep all the information
                "value": f"{row['id']}#{row['symbol']}#{row['name']}",
            }
            for _, row in companies_df.iterrows()
        ],
        placeholder='Select a company...',
        value=None,
        multi=True,
        style={'flex': '1', 'minWidth': '350px'}  # Flex and minimum width
    )


def period_dropdown() -> dcc.Dropdown:
    """
    Dropdown to select the period to display. (10min, 1h, 1d, ...)

    return: dcc.Dropdown
    """
    return dcc.Dropdown(
        id="period-dropdown",
        options=[
            # TODO check 10min, nothing is displayed when chosen
            {"label": "10min", "value": "10min"},
            {"label": "1h", "value": "1h"},
            {"label": "1d", "value": "1d"},
            {"label": "1w", "value": "1W"},
            {"label": "1m", "value": "1ME"},
            {"label": "1y", "value": "1YE"},
        ],
        value="1d",
        style={'flex': '1', 'minWidth': '50px'}  # Flex and minimum width
    )


def date_range_picker() -> dcc.DatePickerRange:
    """
    Date range picker to select the date range to display.

    :return: dcc.DatePickerRange
    """
    return dcc.DatePickerRange(
        id="date-range-picker",
        style={ 'minWidth': '100px'}  # Flex and minimum width
    )


def plot_style_dropdown() -> dcc.Dropdown:
    """
    Dropdown to select the plot style: candlestick, line...

    :return: dcc.Dropdown
    """
    return dcc.Dropdown(
        id="plot-style-dropdown",
        options=[
            {"label": "Candlestick", "value": "candlestick"},
            {"label": "Line", "value": "line"},
        ],
        value="candlestick",
        style={'flex': '1', 'minWidth': '50px'}  # Flex and minimum width
    )


def scale_dropdown() -> dcc.RadioItems:
    """
    Dropdown to select the scale: linear, log...

    :return: dcc.Dropdown
    """
    return dcc.RadioItems(
        id="scale-dropdown",
        options=[
            {"label": "Linear", "value": "linear"},
            {"label": "Log", "value": "log"},
        ],
        value="linear",
        style={'flex': '1', 'minWidth': '20px'}  # Flex and minimum width
    )


def indicators_dropdown() -> dcc.Dropdown:
    """
    Multi dropdown to select the indicators to display.

    :return: dcc.Dropdown
    """
    return dcc.Dropdown(
        id="indicators-dropdown",
        options=[
            {"label": "Bollinger Bands", "value": "bollinger-bands"},
        ],
        value=[],
        multi=True,
        style={'flex': '1', 'minWidth': '100px'}  # Flex and minimum width
    )

# TODO fix deprecated plotly.graph_objects
def go_candlestick(ohlc_df: pd.DataFrame, name: str) -> go.Candlestick:
    candlestick = go.Candlestick(
        x=ohlc_df.index,
        open=ohlc_df["open"],
        high=ohlc_df["high"],
        low=ohlc_df["low"],
        close=ohlc_df["close"],
        # text=symbol,
        name=name,
    )

    return candlestick


def go_line(stocks_df: pd.DataFrame, cid: float, name: str) -> go.Line:
    stocks = stocks_df[stocks_df["cid"] == cid]

    line = go.Line(
        x=stocks.index,
        y=stocks["value"],
        name=name,
    )

    return line


@app.callback(
    ddep.Output("indicator-stock", "children"),
    [
        ddep.Input("companies-dropdown", "value"),
    ],
)
def stock_used_for_indicator(selected_companies) -> RadioItems | None:
    if selected_companies is None or len(selected_companies) == 0:
        return None

    return dcc.RadioItems(
        id="indicator-stock-cid",
        options=[{"label": info.split("#")[2], "value": info.split("#")[0]} for info in selected_companies],
    )


@app.callback(
    ddep.Output("selected-companies-plot", "figure"),
    [
        ddep.Input("companies-dropdown", "value"),
        ddep.Input("period-dropdown", "value"),
        ddep.Input("date-range-picker", "start_date"),
        ddep.Input("date-range-picker", "end_date"),
        ddep.Input("plot-style-dropdown", "value"),
        ddep.Input("scale-dropdown", "value"),
        ddep.Input("indicators-dropdown", "value"),
        ddep.Input("indicator-stock-cid", "value"),
    ],
)
def update_selected_companies_plot(
        selected_values: list[str],
        period: str,
        start_date,
        end_date,
        plot_style: str,
        scale: str,
        indicators: list[str],
        indicator_stock_cid: str | int | None,
) -> go.Figure:
    if selected_values is None or len(selected_values) == 0:
        return go.Figure()

    start_date = start_date or date(1970, 1, 1)
    end_date = end_date or date(2100, 1, 1)

    # TODO find a better way to keep all the information
    selected_cids = [int(cid.split("#")[0]) for cid in selected_values]

    query = (
        f"SELECT *"
        f" FROM stocks"
        f" WHERE cid IN ({', '.join(map(str, selected_cids))})"
        f" AND date BETWEEN '{start_date}' AND '{end_date}'"
    )
    stocks_df = pd.read_sql(query, engine)
    stocks_df.set_index("date", inplace=True)
    stocks_df.sort_index(inplace=True)

    graph_figure_data = []

    if plot_style == "candlestick":
        for info in selected_values:
            cid, symbol, name = info.split("#")
            ohlc_df = (
                stocks_df[stocks_df["cid"] == float(cid)]
                .resample(period)
                .agg(
                    {
                        "value": "ohlc",
                    }
                )
            )
            ohlc_df.dropna(inplace=True)
            ohlc_df.columns = ohlc_df.columns.droplevel()

            graph_figure_data.append(go_candlestick(ohlc_df, name))
    elif plot_style == "line":
        # TODO resample with period use mean
        for info in selected_values:
            cid, _, name = info.split("#")
            graph_figure_data.append(go_line(stocks_df, float(cid), name))

    if "bollinger-bands" in indicators and indicator_stock_cid is not None:
        # TODO resample with period use mean
        stocks_df = stocks_df[
            stocks_df["cid"] == float(indicator_stock_cid)
            ]  # only work when using float, don't know why

        stocks_df["MA20"] = stocks_df.value.rolling(window=20).mean()
        stocks_df["STD20"] = stocks_df.value.rolling(window=20).std()
        stocks_df["upper"] = stocks_df["MA20"] + (stocks_df["STD20"] * 2)
        stocks_df["lower"] = stocks_df["MA20"] - (stocks_df["STD20"] * 2)

        graph_figure_data += [
            go.Line(x=stocks_df.index, y=stocks_df["MA20"], name="MA20"),
            go.Scatter(
                x=stocks_df.index,
                y=stocks_df["upper"],
                name="upper bollinger",
                fill="tonexty",
                line_color="lightblue",
                opacity=0.3,
            ),
            go.Scatter(
                x=stocks_df.index,
                y=stocks_df["lower"],
                name="lower bollinger",
                fill="tonexty",
                line_color="lightblue",
                opacity=0.3,
            ),
        ]

    fig = go.Figure(
        data=graph_figure_data,
    )

    if scale == "log":
        fig.update_yaxes(type="log")
    elif scale == "linear":
        pass

    return fig


@app.callback(
    ddep.Output("selected-companies-table", "children"),
    [
        ddep.Input("companies-dropdown", "value"),
    ],
)
def update_selected_companies_table(selected_values) -> html.Div:
    if selected_values is None or len(selected_values) == 0:
        return html.Div()

    # TODO find a better way to keep all the information
    selected_cids = [int(cid.split("#")[0]) for cid in selected_values]

    query = f"SELECT * FROM daystocks WHERE cid IN ({', '.join(map(str, selected_cids))})"
    stocks_df = pd.read_sql(query, engine)

    # TODO add mean, std, volume when they are added in analyzer
    table_columns = ["date", "low", "high", "open", "close", "mean", "std", "volume"]

    tabs_content = []

    for selected_company in selected_values:
        # Split the company information
        company_id, _, company_name = selected_company.split("#")

        # Query data for the current company
        query = f"SELECT * FROM daystocks WHERE cid = {company_id}"
        stocks_df = pd.read_sql(query, engine)

        # Generate table content for the current company
        table_content = html.Div([
            html.Div([
                html.Table([
                    html.Thead([html.Tr([html.Th(col) for col in table_columns])]),
                    html.Tbody([
                        html.Tr([html.Td(row[col]) for col in table_columns]) for _, row in stocks_df.iterrows()
                    ]),
                ]),
            ], style={'height': 'calc(100vh - 100px)', 'overflowY': 'auto'})
        ])

        # Append the table content to the list of tabs content
        tabs_content.append(
            dcc.Tab(
                label=f"{company_name}",
                children=[table_content],
                value=f"company-{company_id}-tab"
            )
        )

    # Return tabs content wrapped in a Tabs component
    return dcc.Tabs(id='tabs', children=tabs_content, value=f"company-{company_id}-tab")

app.layout = html.Div(
    [
        html.Div(
            [
                html.Div(
                    [
                        html.Div(
                            [
                                companies_dropdown(),
                                period_dropdown(),
                                date_range_picker(),
                                plot_style_dropdown(),
                                indicators_dropdown(),
                                html.Div(id="indicator-stock"),
                            ],
                            className="top-panel",
                        ),
                        dcc.Graph(id="selected-companies-plot"),
                        scale_dropdown(),

                    ],
                    className="panel left-panel",
                ),
                html.Div(
                    [
                        html.Div(id="selected-companies-table"),
                    ],
                    className="panel right-panel",
                ),
            ],
            className="container",
        ),
    ]
)


app.css.append_css({"external_url": "./assets/style.css"})

if __name__ == "__main__":
    app.run(debug=True)
