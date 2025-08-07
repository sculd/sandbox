import pandas as pd, numpy as np
import collections
import datetime

class DailyReport:
    def __init__(self):
        self.realized_pnl_per_strategy = collections.defaultdict(float)
        self.unrealized_pnl_per_strategy = collections.defaultdict(float)
        self.total_pnl_per_strategy = collections.defaultdict(float)
        self.fx_impact_per_strategy = collections.defaultdict(float)

    def update_pnl(self, strategy, realized_pnl, unrealized_pnl):
        self.realized_pnl_per_strategy[strategy] += realized_pnl
        self.unrealized_pnl_per_strategy[strategy] += unrealized_pnl
        self.total_pnl_per_strategy[strategy] += realized_pnl + unrealized_pnl

    def update_fx_impact(self, strategy, fx_impact):
        if fx_impact:
            self.fx_impact_per_strategy[strategy] += fx_impact

    def __str__(self):
        return "\n".join([f"{strategy}: realized: {round(realized_pnl, 1)}, "
                          f"unrealized: {round(self.unrealized_pnl_per_strategy[strategy], 1)}, "
                          f"total: {round(self.total_pnl_per_strategy[strategy], 1)}, "
                          f"fx_impact: {round(self.fx_impact_per_strategy[strategy], 1)}" \
                   for strategy, realized_pnl in self.realized_pnl_per_strategy.items()])

class Report:
    def __init__(self):
        self.daily_report = collections.defaultdict(DailyReport)

    def update_pnl(self, date_str, strategy, realized_pnl, unrealized_pnl):
        self.daily_report[date_str].update_pnl(strategy, realized_pnl, unrealized_pnl)

    def update_fx_impact(self, date_str, strategy, fx_impact):
        self.daily_report[date_str].update_fx_impact(strategy, fx_impact)

    def __str__(self):
        return "\n".join([f"{date_str}\n{pnl}" for date_str, pnl in self.daily_report.items()])


def analyze(df_price_history, df_fx_rates, df_executions):
    report = Report()

    for (symbol, date, strategy), df_exs in df_executions.groupby(['symbol', 'date', "strategy"]):
        #print(f"{symbol=}, {date=}, {strategy=}")
        #print(df_exs[["timestamp", "strategy", "symbol", "side", "price", "quantity"]])

        open_positions = collections.deque()
        realized_pnl = 0
        for _, e in df_exs.iterrows():
            remaining_quantity = e.quantity
            while open_positions and open_positions[0]["side_v"] != e.side_v:
                open_p = open_positions.popleft()
                traded_q = min(open_p["quantity"], remaining_quantity)
                trading_pnl = open_p["side_v"] * (e.price - open_p["price"]) * traded_q
                realized_pnl += trading_pnl
                
                open_p["quantity"] -= traded_q
                if open_p["quantity"]:
                    open_positions.appendleft(open_p)

                remaining_quantity -= traded_q
                if not remaining_quantity:
                    break

            if remaining_quantity:
                open_positions.append({"timestamp": e.timestamp, "side_v": e.side_v, "price": e.price, "quantity": remaining_quantity})

        eod_price = df_price_history.loc[(date, symbol)].close_price
        unrealized_pnl = 0
        open_quantity = 0
        open_position_value = 0
        while open_positions:
            open_p = open_positions.pop()
            open_quantity += open_p["quantity"]
            open_position_value += open_p["side_v"] * open_p["price"] * open_p["quantity"]
            unrealized_pnl += open_p["side_v"] * (eod_price - open_p["price"]) * open_p["quantity"]

        eur_to_usd = df_fx_rates.loc[date].eur_usd
        realized_pnl_usd, unrealized_pnl_usd = realized_pnl * eur_to_usd, unrealized_pnl * eur_to_usd
        #print(f"{eod_price=}, {open_quantity=}, {realized_pnl=}, {unrealized_pnl=}, total_pnl: {realized_pnl + unrealized_pnl}")
        report.update_pnl(date.strftime("%Y-%m-%d"), strategy, realized_pnl_usd, unrealized_pnl_usd)

        next_date = date + datetime.timedelta(days=1)
        if next_date in df_fx_rates.index:
            next_eur_to_usd = df_fx_rates.loc[next_date].eur_usd
            fx_impact = open_position_value * (next_eur_to_usd - eur_to_usd)
            report.update_fx_impact(next_date.strftime("%Y-%m-%d"), strategy, fx_impact)
        else:
            fx_impact = None

    return report


if __name__ == "__main__":
    df_price_history = pd.read_csv("pnl_attribution_mock_multi_day/price_history.csv")
    df_price_history["date"] = pd.to_datetime(df_price_history["date"]).dt.date
    df_price_history = df_price_history.set_index(["date", "symbol"])
    df_fx_rates = pd.read_csv("pnl_attribution_mock_multi_day/fx_rates.csv")
    df_fx_rates["date"] = pd.to_datetime(df_fx_rates["date"]).dt.date
    df_fx_rates = df_fx_rates.set_index("date")
    df_executions = pd.read_csv("pnl_attribution_mock_multi_day/executions.csv").sort_values(["timestamp"])
    df_executions["timestamp"] = pd.to_datetime(df_executions.timestamp)
    df_executions["date"] = df_executions.timestamp.dt.date
    df_executions["side_v"] = np.where(df_executions.side == "SELL", -1, +1)

    report = analyze(df_price_history, df_fx_rates, df_executions)
    print(report)
