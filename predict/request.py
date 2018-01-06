
class Request:
    id = -1
    src = -1
    dest = -1
    size = 0
    req = 0
    ready = 0
    predict = 0  #whether this is a predicted request or not
	
    def copy(self, r):
        self.id = r.id
        self.src = r.src
        self.dest = r.dest
        self.size = r.size
        self.req = r.req
        self.ready = r.ready
        self.predict = r.predict

    def __init__(self, id, src, dest, size, req, ready, predict=0):
        self.id = id
        self.src = src
        self.dest = dest
        self.size = size
        self.req = req
        self.ready = ready
        self.predict = predict

    def print_request(self):
        print self.id, self.src, self.dest, self.ready, self.req


