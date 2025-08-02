import threading
import queue
import time

class Logger:
    def __init__(self):
        self.q = queue.Queue()
        self.loop = threading.Thread(target=self._loop_consume)
        self.loop.start()

    def _loop_consume(self):
        while True:
            msg = self.q.get()
            self.q.task_done()
            if msg is None:
                print("Signal received, stopping the loop")
                break
            self._flush(msg)
            
    def _flush(self, msg):
        decorated = f"{time.time():.2f} {msg}"
        print(decorated)        

    def log(self, msg):
        self.q.put(msg)

    def close(self):
        print("closing the logger")
        self.q.put(None)
        print("wating for the queue cleanup")
        self.q.join()
        self.loop.join()


logger = Logger()
for i in range(10):
    logger.log(f"message {i}")
logger.close()



