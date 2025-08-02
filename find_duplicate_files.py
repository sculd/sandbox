from collections import defaultdict
from typing import List
import hashlib
import os.path
import threading


def compute_streaming_hash_hex(file_name):
    hasher = hashlib.sha256()
    with open(file_name, 'rb') as f:
        while chunk := f.read(1024):
            hasher.update(chunk)
    return hasher.hexdigest()


def compute_streaming_head_hash_hex(file_name):
    hasher = hashlib.sha256()
    with open(file_name, 'rb') as f:
        hasher.update(f.read(1024))
    return hasher.hexdigest()


def is_duplicate(file_name1, file_name2):
    with open(file_name1, 'rb') as f1, open(file_name2, 'rb') as f2:
        while True:
            chunk1 = f1.read(1024)
            chunk2 = f2.read(1024)
            if not chunk1 and not chunk2:
                return True
            if chunk1 != chunk2:
                return False


def full_split_duplicates(filenames):
    if len(filenames) == 1:
        return [filenames]

    ret = [[filenames[0]]]
    for i in range(1, len(filenames)):
        group_found = False
        for g in ret:
            if is_duplicate(filenames[i], g[0]):
                g.append(filenames[i])
                group_found = True
                break
            
        if not group_found:
            ret.append([filenames[i]])

    return ret


class Solution:
    def __init__(self):
        self.cond = threading.Condition()
        self.lock = threading.Lock()
        self.capacity = 4

    def _reset(self):
        self.filenames = []
        self.key_to_paths = defaultdict(list)
        self.filenames_list = []
        self.filenames_examined_list = []

    def _hash_loop(self):
        while True:
            with self.cond:
                self.cond.wait_for(lambda: self.capacity > 0 and self.filenames, timeout=1)
                if not self.filenames:
                    break
                if self.capacity == 0:
                    continue

                with self.lock:
                    filename = self.filenames.pop()
                    self.capacity -= 1

                def hash_and_add(filename):
                    hasher = hashlib.sha256()
                    with open(filename, "rb") as f:
                        while chk := f.read(1024):
                            hasher.update(chk)
                    hash = hasher.hexdigest()
                        
                    key = f"{os.path.getsize(filename)} {hash}"
                    with self.lock:
                        self.key_to_paths[key].append(filename)
                        self.capacity += 1

                    with self.cond:
                        self.cond.notify_all()

                threading.Thread(target=hash_and_add, args=[filename]).start()

    def _scan_duplicates_loop(self):
        while True:
            with self.cond:
                self.cond.wait_for(lambda: self.capacity > 0 and self.filenames_list, timeout=1)
                if not self.filenames_list:
                    break
                if self.capacity == 0:
                    continue

                with self.lock:
                    filenames = self.filenames_list.pop()
                    self.capacity -= 1

                def full_split_duplicates(filenames):
                    try:
                        if len(filenames) == 1:
                            with self.lock:
                                self.filenames_examined_list.append(filenames)
                            return

                        ret = [[filenames[0]]]
                        for i in range(1, len(filenames)):
                            group_found = False
                            for g in ret:
                                if is_duplicate(filenames[i], g[0]):
                                    g.append(filenames[i])
                                    group_found = True
                                    break
                                
                            if not group_found:
                                ret.append([filenames[i]])

                        with self.lock:
                            for g in ret:
                                self.filenames_examined_list.append(g)
                    finally:
                        with self.lock:
                            self.capacity += 1
                        with self.cond:
                            self.cond.notify_all()

                threading.Thread(target=full_split_duplicates, args=[filenames]).start()

    def findDuplicate(self, paths: List[str]) -> List[List[str]]:
        self._reset()
        for info in paths:
            d, fs = info.split(maxsplit=1)
            for f in fs.split():
                filename = f"{d}/{f}"
                with self.lock:
                    self.filenames.append(filename)

                hasher = hashlib.sha256()
                with open(filename, "rb") as f:
                    while chk := f.read(1024):
                        hasher.update(chk)
                hash = hasher.hexdigest()
                    
                key = f"{os.path.getsize(filename)} {hash}"
                self.key_to_paths[key].append(filename)


        #hash_loop_thread = threading.Thread(target=self._hash_loop)
        #hash_loop_thread.start()
        #hash_loop_thread.join()

        for filenames in self.key_to_paths.values():
            self.filenames_list.append(filenames)

        scan_duplicates_loop_thread = threading.Thread(target=self._scan_duplicates_loop)
        scan_duplicates_loop_thread.start()
        scan_duplicates_loop_thread.join()

        return self.filenames_examined_list


    def findDuplicate_seq(self, paths: List[str]) -> List[List[str]]:
        key_to_paths = defaultdict(list)
        for info in paths:
            d, fs = info.split(maxsplit=1)
            for f in fs.split():
                file_name = f"{d}/{f}"

                size = os.path.getsize(file_name)
                hash = compute_streaming_head_hash_hex(file_name)
                key = f"{size} {hash}"
                key_to_paths[key].append(file_name)


        ret = []
        for filenames in key_to_paths.values():
            if len(filenames) == 1:
                continue
            for g in full_split_duplicates(filenames):
                ret.append(g)
        return ret


if __name__ == "__main__":
    solution = Solution()
    paths = ["data/a 1.txt 2.txt","data/c 3.txt",
             "data/c/d 4.txt",
             "data 4.txt"]
    print(solution.findDuplicate(paths))


