import random
import collections

class SetWithRandom:
    def __init__(self):
        self.v_list = []
        self.v_to_i = {}

    def insert(self, v):
        r = v not in self.v_to_i
        if not r:
            return r
        self.v_to_i[v] = len(self.v_list)
        self.v_list.append(v)
        return r

    def remove(self, v):
        r = v in self.v_to_i
        if not r:
            return r
        
        u = self.v_list[-1]
        self.v_to_i[u] = self.v_to_i[v]
        self.v_list[self.v_to_i[u]] = u
        self.v_list.pop()
        self.v_to_i.pop(v)
        return r

    def get_random(self):
        if not self.v_list:
            return -1
        return self.v_list[random.randint(0, len(self.v_list)-1)]


class RandomizedCollection:
    def __init__(self):
        self.v_list = []
        self.v_to_is = collections.defaultdict(set)

    def insert(self, v):
        r = v not in self.v_to_is

        self.v_to_is[v].add(len(self.v_list))
        self.v_list.append(v)
        return r

    def remove(self, v):
        if v not in self.v_to_is:
            return False
        
        u = self.v_list.pop()
        self.v_to_is[u].remove(len(self.v_list))
        if u != v:
            i_v = self.v_to_is[v].pop()
            self.v_to_is[u].add(i_v)
            self.v_list[i_v] = u
        
        if not self.v_to_is[v]:
            self.v_to_is.pop(v)
        return True

    def get_random(self):
        if not self.v_list:
            return -1
        return self.v_list[random.randint(0, len(self.v_list)-1)]



if __name__ == "__main__":
    s = SetWithRandom()
    s.get_random()
    s.insert(1)
    s.insert(3)
    s.insert(2)
    s.remove(5)
    s.insert(5)
    s.insert(1)
    s.remove(5)
    s.get_random()
    s.get_random()

    rc = RandomizedCollection()
    rc.insert(1)
    rc.remove(1)
    rc.insert(1)



