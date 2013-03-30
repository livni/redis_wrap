
from redis_systems import *

class BitsetFu (SetOperators, redis_obj):

    def add(self, item):
        self.conn.setbit(self.name, item, True)

    def remove(self, item):
        r = self.conn.setbit(self.name, item, False)
        if not r:
            raise KeyError

    def discard(self, item):
        self.conn.setbit(self.name, item, False)

    def update(self, other):
        if isinstance(other, BitsetFu):
            self.conn.bitop('OR', self.name, self.name, other.name)
        else:
            super(BitsetFu, self).update(other)

    def intersection_update(self, other):
        if isinstance(other, BitsetFu):
            self.conn.bitop('AND', self.name, self.name, other.name)
        else:
            super(BitsetFu, self).intersection_update(other)

    def symmetric_difference_update(self, other):
        if isinstance(other, BitsetFu):
            self.conn.bitop('XOR', self.name, self.name, other.name)
        else:
            super(BitsetFu, self).symmetric_difference_update(other)

    def __len__(self):
        return self.conn.bitcount(self.name)

    def __iter__(self):
        for i in range(self.conn.strlen(self.name)*8):
            if self.conn.getbit(self.name, i):
                yield i

    def __contains__(self, item):
        return self.conn.getbit(self.name, item)

