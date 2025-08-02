import asyncio
import time

################################

async def task(n):
    print(f"Task {n} start")
    await asyncio.sleep(1)
    print(f"Task {n} end")
    return n

async def fetch_data(i):
    print(f"Start fetching {i}")
    await asyncio.sleep(1)  # Non-blocking delay
    #time.sleep(1)
    print(f"Done fetching {i}")
    result = {"data": i}
    print(result)
    return result

################################

async def main_awaits():
    time.sleep(1)
    print("main_awaits")
    print(await task(1))
    print(await task(2))

#asyncio.run(main_awaits())

################################

async def main_gather():
    time.sleep(1)
    print("main_gather")
    print(await asyncio.gather(task(1), task(2)))

#asyncio.run(main_gather())

################################

async def main_create_task():
    time.sleep(1)
    print("main_create_task")
    t1 = asyncio.create_task(task(1))
    t2 = asyncio.create_task(task(2))

    print("Waiting for tasks to complete...")
    print(await t1, await t2)

#asyncio.run(main_create_task())


async def task_sleep(i):
    try:
        await asyncio.sleep(i)
        print(f"task {i} done")
    except asyncio.CancelledError:
        print(f"task {i} was cancelled")
        raise

async def main_create_long_tasks():
    t1 = asyncio.create_task(task_sleep(1))
    t2 = asyncio.create_task(task_sleep(5))  # 오래 걸리게
    print("main done")

#asyncio.run(main_create_long_tasks())


################################

async def main_gather_no_await():
    time.sleep(1)
    print("main_gather_no_await")
    asyncio.gather(task(1), task(1))
    #await asyncio.sleep(2)

asyncio.run(main_gather_no_await())


################################


################################


################################


