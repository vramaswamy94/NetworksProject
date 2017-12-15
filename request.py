
class Request:
    id = -1
    src = -1
    dest = -1
    size = 0
    req = 0
    ready = 0 

    def copy(self, r):
        self.id = r.id
        self.src = r.src
        self.dest = r.dest
        self.size = r.size
        self.req = r.req
        self.ready = r.ready

    def __init__(self, id, src, dest, size, req, ready):
        self.id = id
        self.src = src
        self.dest = dest
        self.size = size
        self.req = req
        self.ready = ready

    def print_request(self):
        print self.id, self.src, self.dest, self.ready, self.req


