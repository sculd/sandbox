import sys
import os


for i in range(4):
    sys.stdout.write(f"{i} ")
    sys.stdout.flush()
    os.fork()

