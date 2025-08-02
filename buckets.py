import asyncio
import time

class TokenBucket:
    def __init__(self, capacity: int, refill_rate: float):
        self.max_capacity = capacity
        self.current_capacity = capacity
        self.refill_rate = refill_rate

    async def _loop_refill_tokens(self):
        while True:
            await asyncio.sleep(1)
            self.current_capacity = min(self.max_capacity, self.current_capacity + self.refill_rate)
            print(f'[_loop_refill_tokens] {self.current_capacity=}')

    async def start(self):
        self.refill_task = asyncio.create_task(self._loop_refill_tokens())

    def terminate(self):
        self.refill_task.cancel()

    def allow_request(self):
        if self.current_capacity < 1:
            print(f'{self.current_capacity=}')
            return False
        print(f'{self.current_capacity=}')
        self.current_capacity -= 1
        return True

class SimplerTokenBucket:
    def __init__(self, capacity: int, refill_rate: float):
        self.max_capacity = capacity
        self.current_capacity = capacity
        self.refill_rate = refill_rate
        self.latest_request = time.monotonic()

    def allow_request(self):
        t = time.monotonic()
        elapsed = t - self.latest_request
        self.latest_request = t

        self.current_capacity = min(self.max_capacity, self.current_capacity + elapsed * self.refill_rate)

        if self.current_capacity < 1:
            print(f'{self.current_capacity=}')
            return False
        print(f'{self.current_capacity=}')
        self.current_capacity -= 1
        return True

async def main():
    tb = SimplerTokenBucket(capacity=2, refill_rate=0.4)  # 5 req/sec, burst up to 10
    #await tb.start()

    for i in range(7):
        print(f"{i=}, {tb.allow_request()}")
        await asyncio.sleep(1.0)

    #tb.terminate()

asyncio.run(main())
