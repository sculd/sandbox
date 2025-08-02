
def count_up_to(n):
    print("Starting count")
    for i in range(1, n + 1):
        print(f"Yielding {i}")
        yield i
        print(f"Yielded {i}")
    print("Ending count")
    return n

vals = count_up_to(3)
print("just created counter")
for val in vals:
    print("Got:", val)


def my_generator():
    print("Generator started")
    value_received = yield "First yielded value"
    print(f"Received: {value_received}")
    value_received_2 = yield "Second yielded value"
    print(f"Received again: {value_received_2}")

# Create the generator object
gen = my_generator()

# Advance to the first yield (or send(None))
first_yielded = next(gen)
print(f"From caller: {first_yielded}")

# Send a value back into the generator
second_yielded = gen.send("Hello from caller!")
print(f"From caller: {second_yielded}")

# Send another value
try:
    gen.send("Another message!")
except StopIteration:
    print("Generator finished.")




def accumulator():
    total = 0
    while True:
        value = yield total
        if value is None:
            break
        total += value

acc = accumulator()
next(acc)           # yield까지 실행
acc.send(10)        # total = 10
acc.send(5)         # total = 15
try:
    acc.send(None)      # 종료
except StopIteration as ex:
    print(ex)



def sink():
    while True:
        msg = yield
        print(f"[sink] received: {msg}")

def filter_even(next_coroutine):
    while True:
        val = yield
        if val % 2 == 0:
            next_coroutine.send(val)

def source(start, end, target):
    for i in range(start, end):
        target.send(i)

s = sink()
print(next(s))

f = filter_even(s)
print(next(f))

source(0, 5, f)


def gen_a():
    yield 1
    yield 2

def gen_b():
    yield from gen_a()
    yield 3


for v in gen_b():
    print(v)



'''
유형	예시 질문
개념	"yield from과 yield 차이를 설명하라. 언제 쓰이는가?"
실용	"stateful generator로 평균을 유지하는 코루틴을 짜보라."
체이닝	"두 generator를 연결해 데이터를 필터링하고 누적하는 구조를 만들어보라."
비판적 사고	"send()를 남용했을 때의 단점은?"
"generator와 iterator 차이는?"

HRT 인터뷰에서 “coroutine”이 언급되면 보통 다음을 묻습니다:
generator 기반 coroutine 체이닝 (stream processing)
yield from을 통한 위임
send()로 외부 데이터 주입
'''


# stateful generator로 평균을 유지하는 코루틴을 짜보라.
def avg_sink():
    tot = 0
    cnt = 0
    avg = 0
    while True:
        v = yield avg
        if v is None:
            break
        tot += v
        cnt += 1
        avg = tot / cnt
        print(f"{tot=}, {cnt=}, {avg=}")

    return avg


def generate_vs(n):
    s = avg_sink()
    next(s)
    for i in range(n):
        print(s.send(i))

    try:
        s.send(None)
    except StopIteration as ex:
        print(f"StopInteration: {ex}")

generate_vs(10)


def subgen():
    received = yield "subgen ready"
    print(f"subgen received: {received}")
    yield "subgen done"

def with_yield():
    sg = subgen()
    yield next(sg)               # "subgen ready"
    received = yield             # 수동으로 send() 받기
    sg.send(received)
    yield next(sg)               # "subgen done"

def with_yield_fixed():
    sg = subgen()
    x = next(sg)     # subgen ready
    val = yield x    # 외부에서 보내준 값을 받아서
    y = sg.send(val) # subgen 내부로 전달
    yield y          # subgen이 다시 yield한 값 전달

def with_yield_from():
    yield from subgen()


print('1. 수동 yield 버전')
g = with_yield_fixed()
print(next(g))            # → "subgen ready"
print(g.send(42))         # → "subgen done" + 내부에서 subgen received 출력

print('2. yield from 버전')
g2 = with_yield_from()
print(next(g2))           # → "subgen ready"
print(g2.send(42))        # → "subgen done" + 동일하게 작동



