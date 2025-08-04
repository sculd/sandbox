import csv
import datetime
import collections

_datetime_format = "%Y-%m-%dT%H:%M:%SZ"

def _parse_tpq(t, p, q):
    if type(t) == str:
        t = datetime.datetime.strptime(t, _datetime_format)
    if type(p) == str:
        p = float(p)
    if type(q) == str:
        q = float(q)
    return t, p, q

class Bar:
    def __init__(self, t, o, h, l, c, q):
        self.t = t.replace(second=0)
        self.o = o
        self.h = h
        self.l = l
        self.c = c
        self.q = q

    def __str__(self):
        return f"{self.t}, ({self.o}, {self.h}, {self.l}, {self.c}), {self.q}"

    def update(self, t, p, q):
        t_mintely = t.replace(second=0)
        if self.t != t_mintely:
            return

        self.h = max(self.h, p)
        self.l = min(self.l, p)
        self.q += q

class BarSeries:
    def __init__(self):
        self.t_latest = datetime.datetime.fromtimestamp(0)
        self.bars = []

    def __str__(self):
        return '\n'.join([str(b) for b in self.bars])

    def is_empty(self):
        return not self.bars

    def _update_bar(self, t, p, q):
        if t - self.t_latest < datetime.timedelta(seconds=-30):
            # drop old message
            pass
        self.t_latest = max(self.t_latest, t)

        if not self.bars:
            return
        
        self.bars[-1].update(t, p, q)
        if len(self.bars) > 1:
            self.bars[-2].update(t, p, q)

    def ingest(self, t, p, q):
        t_mintely = t.replace(second=0)
        if self.is_empty() or t_mintely > self.bars[-1].t:
            self.bars.append(Bar(t, p, p, p, p, q))
        else:
            self._update_bar(t, p, q)

class Aggregator:
    def __init__(self):
        self.bars = collections.defaultdict(BarSeries)

    def get_latest_bar(self, symbol):
        if self.bars[symbol].is_empty():
            return None
        return self.bars[symbol][-1]

    def aggregate(self, file_name):
        with open(file_name, "r") as f:
            reader = csv.DictReader(f)

            #rows = [row for row in reader]
            for row in reader:
                try:
                    s, (t, p, q) = row['symbol'], _parse_tpq(row['timestamp'], row['price'], row['quantity'])
                except Exception as ex:
                    print(ex)
                    continue

                self.bars[s].ingest(t, p, q)


if __name__ == "__main__":
    a = Aggregator()
    a.aggregate("tick_aggregator_mock/ticks.csv")

    for s, bars in a.bars.items():
        print(s)
        print(bars)

