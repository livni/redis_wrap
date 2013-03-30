import redis


#--- System related ----------------------------------------------
SYSTEMS = {
    'default': redis.Redis(host='localhost', port=6379)
}

def setup_system(name, host, port, **kw):
    SYSTEMS[name] = redis.Redis(host=host, port=port, **kw)

def get_redis(system='default'):
    return SYSTEMS[system]


class redis_obj(object):

    def __init__(self, name, system='default', persistent=True):
        self.name = name
        self.system = system
        self.conn = get_redis(system)
        self.persistent = persistent

    def clear(self):
        self.conn.delete(self.name)

    def __del__(self):
        if not self.persistent:
            self.conn.delete(self.name)


class SetOperators(object):
    type_name = ''

    def update(self, other):
        for item in other:
            self.add(item)

    def intersection_update(self, other):
        for item in self:
            if item not in other:
                self.discard(item)

    def difference_update(self, other):
        for item in other:
            self.discard(item)

    def symmetric_difference_update(self, other):
        for item in other:
            if item in self:
                self.remove(item)
            else:
                self.add(item)

    def __isub__(self, other):
        self.difference_update(other)
        return self

    def __sub__(self, other):
        newset = self.__class__('%s_oper_sub_%s-%s'%(self.type_name, self.name, other.name),
            self.system, persistent=False)
        newset.update(self)
        newset -=other
        return newset

    def __iand__(self, other):
        self.intersection_update(other)
        return self

    def __and__(self, other):
        newset = self.__class__('%s_oper_and_%s-%s'%(self.type_name, self.name, other.name),
            self.system, persistent=False)
        newset.update(self)
        newset &= other
        return newset

    def __ixor__(self, other):
        self.symmetric_difference_update(other)
        return self

    def __xor__(self, other):
        newset = self.__class__('%s_oper_xor_%s-%s'%(self.type_name, self.name, other.name),
            self.system, persistent=False)
        newset.update(self)
        newset ^= other
        return newset

    def __ior__(self, other):
        self.update(other)
        return self

    def __or__(self, other):
        newset = self.__class__('%s_oper_or_%s-%s'%(self.type_name, self.name, other.name),
            self.system, persistent=False)
        newset.update(self)
        newset |= other
        return newset

