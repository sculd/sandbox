d = {}
d['x'] = 10
d['y'] = 20
d['z'] = 30
d.pop('x')
d['x'] = 40

for k, v in d.items():
    print(k, v)
