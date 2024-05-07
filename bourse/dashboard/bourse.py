import dash
from dash import dcc
from dash import html
import dash.dependencies as ddep
import pandas as pd
import sqlalchemy
import plotly.graph_objects as go
import plotly.io as pio
from datetime import date
from dash import Input, Output, State, html
import dash_bootstrap_components as dbc
import dash_daq as daq


from dash.dcc import RadioItems

external_stylesheets = [
    "https://codepen.io/chriddyp/pen/bWLwgP.css",
    "https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@20..48,100..700,0..1,-50..200",
    # "https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@24,400,0,0",
]

theme_name = "plotly"

DATABASE_URI = "timescaledb://ricou:monmdp@db:5432/bourse"  # inside docker
# DATABASE_URI = 'timescaledb://ricou:monmdp@localhost:5432/bourse'  # outisde docker
engine = sqlalchemy.create_engine(DATABASE_URI)

app = dash.Dash(
    __name__,
    title="Bourse",
    suppress_callback_exceptions=True,
    external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.FONT_AWESOME] + external_stylesheets,
)
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
                "value": f"{row['id']}#{row['symbol']}#{row['name']}",
            }
            for _, row in companies_df.iterrows()
        ],
        placeholder="Select a company...",
        value=None,
        multi=True,
        style={"flex": "1", "minWidth": "350px"},  # Flex and minimum width
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
            {"label": "10m", "value": "10min"},
            {"label": "1h", "value": "1h"},
            {"label": "1d", "value": "1d"},
            {"label": "1w", "value": "1W"},
            {"label": "1m", "value": "1ME"},
            {"label": "1y", "value": "1YE"},
        ],
        clearable=False,
        value="1d",
        maxHeight=300,
        style={"flex": "1", "maxWidth": "60px", "maxHeight": "50px", "border": "none"},  # Flex and minimum width
    )

def date_range_picker() -> dcc.DatePickerRange:
    """
    Date range picker to select the date range to display.

    :return: dcc.DatePickerRange
    """
    return dcc.DatePickerRange(id="date-range-picker", style={"minWidth": "100px", "border": "none"})


def plot_style_dropdown() -> dcc.Dropdown:
    """
    Dropdown to select the plot style: candlestick, line...

    :return: dcc.Dropdown
    """
    return dcc.Dropdown(
        id="plot-style-dropdown",
        options=[
            {
                "label": html.Div(
                    [
                        html.Span(className="material-symbols-outlined", children="candlestick_chart"),
                        html.Span("Candles"),
                    ],
                    id="candle-option",
                    className="dropdown-option",
                ),
                "value": "candlestick",
            },
            {
                "label": html.Div(
                    [
                        html.Span(className="material-symbols-outlined", children="show_chart"),
                        html.Span("Line"),
                    ],
                    id="line-option",
                    className="dropdown-option",
                ),
                "value": "line",
            },
        ],
        value="candlestick",
        clearable=False,
        style={"flex": "1", "maxWidth": "120px", "border": "none"},  # Flex and minimum width
    )


def scale_dropdown() -> dcc.Dropdown:
    """
    Dropdown to select the scale: linear, log...

    :return: dcc.Dropdown
    """
    return dcc.Dropdown(
        id="scale-dropdown",
        options=[
            {"label": "Linear", "value": "linear"},
            {"label": "Log", "value": "log"},
        ],
        value="linear",
        clearable=False,
        style={"flex": "1", "maxWidth": "120px", "border": "none"},  # Flex and minimum width
    )


def indicators_dropdown() -> dcc.Dropdown:
    """
    Multi dropdown to select the indicators to display.

    :return: dcc.Dropdown
    """
    return html.Div(
        dcc.Dropdown(
            id="indicators-dropdown",
            placeholder="Select indicator",
            options=[
                {"label": "Bollinger Bands", "value": "bollinger-bands"},
            ],
            value=[],
            multi=True,
            clearable=True,
            style={"flex": "1", "minWidth": "150px", "border": "none"},  # Flex and minimum width
        )
    )


# TODO fix deprecated plotly.graph_objects
def go_candlestick(ohlc_df: pd.DataFrame, name: str) -> go.Candlestick:
    candlestick = go.Candlestick(
        x=ohlc_df.index,
        open=ohlc_df["open"],
        high=ohlc_df["high"],
        low=ohlc_df["low"],
        close=ohlc_df["close"],
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
        ddep.Input("darktheme-daq-booleanswitch", "on"),
    ],
)
def stock_used_for_indicator(selected_companies, dark_mode) -> dcc.Dropdown | None:
    if selected_companies is None or len(selected_companies) == 0:
        return None

    return dcc.Dropdown(
        id="indicator-stock-cid",
        placeholder="On company",
        clearable=True,
        options=[{"label": info.split("#")[2], "value": info.split("#")[0]} for info in selected_companies],
        className = "drop-down-dark" if dark_mode else "drop-down",
        style={"flex": "1", "minWidth": "200px", "border": "none"},  # Flex and minimum width
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
        ddep.Input("darktheme-daq-booleanswitch", "on"),
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
        dark_mode_on,
) -> go.Figure:
    template_name = theme_name + "_dark" if dark_mode_on else theme_name
    if selected_values is None or len(selected_values) == 0:
        fig = go.Figure()
        fig.update_layout(template=template_name)
        return fig

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
    fig.update_layout(template=template_name)

    if scale == "log":
        fig.update_yaxes(type="log")
    elif scale == "linear":
        pass

    rangebreaks = []
    if period not in ["1w", "1m", "1y"]:
        # choosing a period that is bigger than a day can result in a "datapoint" that start a weekend day
        rangebreaks.append({'pattern': 'day of week', 'bounds': [6, 1]})
    if period in ["10min", "1h"]:
        # choosing a period bigger than an hour result in datapoint starting a 00:00am, that would be removed
        rangebreaks.append({'pattern': 'hour', 'bounds': [18, 9]})

    fig.update_xaxes(
        rangebreaks=rangebreaks,
    )

    return fig

def format_table_cell(value: any, column_name: str) -> str:
    if column_name == "date":
        return value.strftime("%Y-%m-%d")
    if column_name in ["low", "high", "open", "close", "mean", "std"]:
        return f"{value:.2f}"
    else:
        return value

@app.callback(
    ddep.Output("selected-companies-table", "children"),
    [
        ddep.Input("companies-dropdown", "value"),
        ddep.Input("date-range-picker", "start_date"),
        ddep.Input("date-range-picker", "end_date"),
        ddep.Input("darktheme-daq-booleanswitch", "on"),
    ],
)
def update_selected_companies_table(selected_values, start_date, end_date, dark_mode) -> html.Div:
    if selected_values is None or len(selected_values) == 0:
        return html.Div()

    # TODO find a better way to keep all the information
    selected_cids = [int(cid.split("#")[0]) for cid in selected_values]

    start_date = start_date or date(1970, 1, 1)
    end_date = end_date or date(2100, 1, 1)

    table_columns = ["date", "low", "high", "open", "close", "mean", "std", "volume"]

    tabs_content = []

    for selected_company in selected_values:
        # Split the company information
        company_id, _, company_name = selected_company.split("#")

        # Query data for the current company
        query = f"SELECT * FROM daystocks"\
                f" WHERE cid = {company_id}"\
                f" AND date BETWEEN '{start_date}'"\
                f" AND '{end_date}'"\
                " ORDER BY date"
        stocks_df = pd.read_sql(query, engine)

        # Generate table content for the current company
        table_content = html.Div(
            [
                html.Div(
                    [
                        html.Table(
                            [
                                html.Tr(
                                    [
                                        html.Th(col, className="th-dark" if dark_mode else "") for col in table_columns
                                    ]
                                ),
                                html.Tbody(
                                    [
                                        html.Tr([html.Td(format_table_cell(row[col], col)) for col in table_columns])
                                        for _, row in stocks_df.iterrows()
                                    ]
                                ),
                            ]
                        ),
                    ],
                    style={"height": "85vh", "overflowY": "auto"},
                )
            ]
        )

        # Append the table content to the list of tabs content
        tabs_content.append(
            dcc.Tab(label=f"{company_name}", className="dark-tabs" if dark_mode else "", children=[table_content], value=f"company-{company_id}-tab")
        )

    # Return tabs content wrapped in a Tabs component
    return dcc.Tabs(id="tabs", children=tabs_content, value=f"company-{company_id}-tab")


@app.callback(
    Output("modal", "is_open"),
    [Input("open", "n_clicks"), Input("close", "n_clicks")],
    [State("modal", "is_open")],
)
def toggle_modal(n1, n2, is_open):
    if n1 or n2:
        return not is_open
    return is_open


@app.callback(
    ddep.Output('companies-dropdown', 'options'),
    [ddep.Input('update-button', 'n_clicks')]
)
def update_dropdown_options(n_clicks):
    companies_df = get_companies()
    options = [
        {'label': f"{row['name']} ({row['symbol']})", 'value': f"{row['id']}#{row['symbol']}#{row['name']}"}
        for _, row in companies_df.iterrows()
    ]
    return options


@app.callback(
    Output("date_modal", "is_open"),
    [Input("date_open", "n_clicks"), Input("date_close", "n_clicks")],
    [State("date_modal", "is_open")],
)
def date_toggle_modal(n1, n2, is_open):
    if n1 or n2:
        return not is_open
    return is_open

@app.callback(
    Output("top-panel", "className"),
    Output("bottom-panel", "className"),
    Output("open", "className"),
    Output("date_open", "className"),
    Output("update-button", "className"),
    Output("left-panel", "className"),
    Output("right-panel", "className"),
    Output("period-dropdown", "className"),
    Output("plot-style-dropdown", "className"),
    Output("scale-dropdown", "className"),
    Output("indicators-dropdown", "className"),
    Output("candle-option", "className"),
    Output("line-option", "className"),
    Output("companies-dropdown", "className"),
    Output("modal", "className"),
    Output("date_modal", "className"),
    Output("selected-companies-table", "className"),
    Output("date-range-picker", "className"),
    [Input("darktheme-daq-booleanswitch", "on")]
)
def dark_mode_style(switch_state):
    if switch_state:
        return "top-panel-dark", "bottom-panel-dark", "squared-button-dark", "squared-button-dark", "squared-button-dark", "panel left-panel-dark", "panel right-panel-dark", "drop-down-dark","drop-down-dark","drop-down-dark", "drop-down-dark", "drop-down-dark", "drop-down-dark", "companies-dropdown-dark", "modal-dark", "modal-dark", "table-content-dark", "dark-mode-date-picker"
    else:
        return "top-panel", "bottom-panel", "squared-button", "squared-button", "squared-button", "panel left-panel", "panel right-panel", "drop-down","drop-down","drop-down", "drop-down", "drop-down", "drop-down", "companies-dropdown", "", "", "", ""

app.layout = html.Div(
    [
        html.Div(
            [
                 daq.BooleanSwitch(
                    on=False,
                    id="darktheme-daq-booleanswitch",
                    className="dark-theme-control",
                
                    color="purple",
                ),
                html.Button(
                    html.I(className="fa-solid fa-arrows-rotate"),
                    id="update-button",
                    className="squared-button",
                ),
                html.Span("", style={"display": "inline-block", "border-left": "2px solid #ccc", "height": "30px"}),
                html.Div(
                    [
                        html.Button(
                            html.I(className="fa-solid fa-magnifying-glass"),
                            id="open",
                            n_clicks=0,
                            className="squared-button",
                        ),
                        dbc.Modal(
                            [
                                dbc.ModalHeader(dbc.ModalTitle("Compare symbol")),
                                dbc.ModalBody(companies_dropdown()),
                                dbc.ModalFooter(dbc.Button("Close", id="close", className="ms-auto", n_clicks=0)),
                            ],
                            id="modal",
                            is_open=False,
                        ),
                    ]
                ),
                html.Span("", style={"display": "inline-block", "border-left": "2px solid #ccc", "height": "30px"}),
                period_dropdown(),
                html.Span("", style={"display": "inline-block", "border-left": "2px solid #ccc", "height": "30px"}),
                plot_style_dropdown(),
                html.Span("", style={"display": "inline-block", "border-left": "2px solid #ccc", "height": "30px"}),
                scale_dropdown(),
                html.Span(
                    "",
                    style={
                        "display": "inline-block",
                        "border-left": "2px solid #ccc",
                        "height": "30px",
                    },
                ),
                
                
                html.Div(
                    [
                        html.Button(
                            html.I(className="fa-regular fa-calendar"),
                            id="date_open",
                            n_clicks=0,
                            className="squared-button",
                        ),
                        dbc.Modal(
                            [
                                dbc.ModalHeader(dbc.ModalTitle("Choose period")),
                                dbc.ModalBody(date_range_picker()),
                                dbc.ModalFooter(dbc.Button("Close", id="date_close", className="ms-auto", n_clicks=0)),
                            ],
                            id="date_modal",
                            is_open=False,
                        ),
                    ]
                ),
                # date_range_picker(),
                html.Span(
                    "",
                    style={
                        "display": "inline-block",
                        "border-left": "2px solid #ccc",
                        "height": "30px",
                    },
                ),
                indicators_dropdown(),
                html.Div(id="indicator-stock"),
            ],
            id="top-panel",  
            className="top-panel",
        ),
        html.Div(
            [
                html.Div(
                    [
                        dcc.Graph(id="selected-companies-plot", style={'height': '80vh'})
                    ],
                    id="left-panel",
                    className="panel left-panel",
                ),
                html.Div(
                    [
                        html.Div(id="selected-companies-table"),
                    ],
                    id="right-panel",
                    className="panel right-panel",
                ),
            ],
            id="bottom-panel",  
            className="bottom-panel",
        ),
        
    ],
)

app.css.append_css({"external_url": "./assets/style.css"})

if __name__ == "__main__":
    app.run(debug=True)
