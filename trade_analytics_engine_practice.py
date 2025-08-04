import datetime
import csv
import collections
import pandas as pd

_datetime_format = "%Y-%m-%dT%H:%M:%SZ"

class CircularWindow:
    def __init__(self, capacity):
        self.elems = [None for _ in range(capacity)]
        self.tail = len(self.elems) - 1

    def append(self, elem):
        self.tail = (self.tail + 1) % len(self.elems)
        cur_elem = self.elems[self.tail]
        self.elems[self.tail] = elem
        return cur_elem

    def _get_tail_i(self, delta=0):
        assert delta <= 0
        l = len(self.elems)
        assert abs(delta) < l
        return (self.tail + delta + l) % l

    def get_tail(self, delta=0):
        return self.elems[self._get_tail_i(delta=delta)]

    def get_as_list(self, l, tail_delta=0):
        return [self.elems[self._get_tail_i(delta=tail_delta-l+1+i)] for i in range(l)]

    def update_tail(self, elem, delta=0):
        i = self._get_tail_i(delta=delta)
        cur_elem = self.elems[i]
        self.elems[i] = elem
        return cur_elem

    def get_vwap(self, vwap_window_size_minutes, tail_delta):
        vwap_elems = self.get_as_list(l=vwap_window_size_minutes, tail_delta=tail_delta)
        vwap_elems = [e for e in vwap_elems if e is not None]
        if vwap_elems:
            vwap = sum([e[1] * e[2] for e in vwap_elems]) / sum([e[2] for e in vwap_elems])
        else:
            vwap = None
        return vwap


def _parse_tpq(t, p, q):
    if type(t) == str:
        t = datetime.datetime.strptime(t, _datetime_format)
    if type(p) == str:
        p = float(p)
    if type(q) == str:
        q = float(q)
    return t, p, q

class Engine:
    def __init__(self, vwap_window_size_minutes=5, grace_period_minutes=3):
        self.vwap_window_size_minutes = vwap_window_size_minutes
        self.grace_period_minutes = grace_period_minutes
        self.circular_window_size_minutes = self.vwap_window_size_minutes + self.grace_period_minutes
        self._reset()

    def _reset(self):
        # avg trade size
        self.total_quantities = collections.defaultdict(float)
        self.n_trades = collections.defaultdict(int)
        self.average_trade_size = {}

        # 5min vwap
        self.vwaps = collections.defaultdict(lambda: [None for _ in range(self.grace_period_minutes)])
        self.windows = collections.defaultdict(collections.deque)
        self.cirular_windows = collections.defaultdict(lambda: CircularWindow(self.circular_window_size_minutes))
        self.pq_sums = collections.defaultdict(float)
        self.q_sums = collections.defaultdict(float)
        self.recent_minutes = {}

    def _ingest_for_trade_size_stat(self, s, q):
        # avg trade size
        self.total_quantities[s] += q
        self.n_trades[s] += 1
        self.average_trade_size[s] = self.total_quantities[s] / self.n_trades[s]

    def ingest_as_stream(self, t, s, p, q):
        t, p, q = _parse_tpq(t, p, q)
        t_minutely = t.replace(second=0)

        # avg trade size
        self._ingest_for_trade_size_stat(s, q)

        # vwap
        elem = (t_minutely, p, q,)
        if s not in self.cirular_windows:
            t_vwap = t_minutely
            t_delta_minutes = 1
        else:
            # from the most recent value
            tail_t = self.cirular_windows[s].get_tail()[0]
            t_vwap = tail_t + datetime.timedelta(minutes=1)
            t_delta = t_minutely - tail_t
            t_delta_minutes = int(t_delta.total_seconds() // 60)

        if t_delta_minutes > 0:
            while t_delta_minutes > 0:
                if t_delta_minutes > 1:
                    self.cirular_windows[s].append(None)
                else:
                    self.cirular_windows[s].append(elem)

                vwap = self.cirular_windows[s].get_vwap(self.vwap_window_size_minutes, tail_delta=0)
                self.vwaps[s].append((t_vwap, vwap,))

                t_delta_minutes -= 1
                t_vwap += datetime.timedelta(minutes=1)

        elif t_delta_minutes <= -self.grace_period_minutes:
            # drop late message
            print(f"late message dropeed: {t}, {s}, {p}, {q}")
        else:
            tail = self.cirular_windows[s].get_tail(delta=t_delta_minutes)
            if tail:
                ct, cp, cq = self.cirular_windows[s].get_tail(delta=t_delta_minutes)
                assert ct == t_minutely, f"{ct=} not same as {t_minutely=}"
                # t, p, q
                sum_elem = (t_minutely, (p*q + cp*cq) / (q+cq), q+cq)
            else:
                sum_elem = (t_minutely, p, q)
            self.cirular_windows[s].update_tail(sum_elem, delta=t_delta_minutes)
            
            t_vwap = t_minutely
            while t_delta_minutes <= 0:
                vwap = self.cirular_windows[s].get_vwap(self.vwap_window_size_minutes, tail_delta=t_delta_minutes)
                i = len(self.vwaps[s])-1+t_delta_minutes
                self.vwaps[s][i] = (t_vwap, vwap,)
                t_delta_minutes += 1
                t_vwap += datetime.timedelta(minutes=1)

    def _prune_window(self, s, windows, pq_sums, q_sums):
        while windows[s]:
            if windows[s][-1][0] - windows[s][0][0] < datetime.timedelta(minutes=self.vwap_window_size_minutes):
                break

            _, lp, lq = windows[s].popleft()
            pq_sums[s] -= lp * lq
            q_sums[s] -= lq

    def ingest(self, t, s, p, q):
        t, p, q = _parse_tpq(t, p, q)
        t_minutely = t.replace(second=0)

        # avg trade size
        self._ingest_for_trade_size_stat(s, q)

        # vwap
        try:
            self.windows[s].append((t_minutely, p, q,))
            self.pq_sums[s] += p * q
            self.q_sums[s] += q
            self._prune_window(s, self.windows, self.pq_sums, self.q_sums)
            
            vwap = round(self.pq_sums[s] / self.q_sums[s], 3)
            if s not in self.recent_minutes or t_minutely > self.recent_minutes[s]:
                self.vwaps[s].append((t_minutely, vwap,))
            else:
                self.vwaps[s][-1] = (t_minutely, vwap,)
            self.recent_minutes[s] = t_minutely
        except Exception as e:
            print(f"exception {e}, for {t}, {s}, {p}, {q}")

    def analyze_file(self, file_name):
        with open(file_name, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)

            #rows = sorted([row for row in reader], key=lambda row: row['timestamp'])
            rows = [row for row in reader]
            for row in rows: 
                try:
                    s, t, p, q = row['symbol'], datetime.datetime.strptime(row['timestamp'], _datetime_format), float(row['price']), float(row['quantity'])
                    #self.ingest(t, s, p, q)
                    self.ingest_as_stream(t, s, p, q)                    
                except Exception as e:
                    print(f"[analyze_file] exception {e}, at {row}")

        return self.average_trade_size, self.vwaps
    
    def analyze_file_as_dataframe(self, file_name):
        df = pd.read_csv(file_name)
        df['quantity'] = df.quantity.astype('float')
        df_average_trade_size = df.groupby(["symbol"])[["quantity"]].mean()

        df['price'] = df.price.astype('float')
        df['timestamp'] = pd.to_datetime(df.timestamp, format="%Y-%m-%dT%H:%M:%SZ")
        df['t_minutely'] = df.timestamp.dt.floor('1min')
        df['pq'] = df.price * df.quantity

        df_minutely = df.groupby(["symbol", "t_minutely"])[["quantity", "pq"]].sum().reset_index().set_index("t_minutely")
        df_minutely_rolled = df_minutely.groupby(["symbol"]).rolling(5).sum()
        df_minutely_rolled["vwap"] = df_minutely_rolled.pq / df_minutely_rolled.quantity

        return df_average_trade_size, df_minutely_rolled

if __name__ == "__main__":
    e = Engine(grace_period_minutes=6)
    average_trade_size, vwaps = e.analyze_file("bam_quant_dev_mock/trades_aapl.csv")
    print("vwaps")
    for symbol, s in average_trade_size.items():
        print(symbol)
        print(s)

    print("vwaps")
    for symbol, vs in vwaps.items():
        print(symbol)
        in_padding = True
        for v in vs:
            if in_padding and v is None:
                continue
            print(v)
            in_padding = False


