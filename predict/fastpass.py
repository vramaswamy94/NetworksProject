from bitarray import bitarray
import Queue
from request import Request

def Matching(request_queue, num_nodes, flowlet):
	used_src = bitarray(num_nodes+1)
	used_src.setall(False)
	used_dest = bitarray(num_nodes+1)
	used_dest.setall(False)
	admitted = Queue.Queue()
	backlog = Queue.Queue()
	admitted_array = [0]*(num_nodes+1)*(num_nodes+1)	

	while not request_queue.empty():
		req = request_queue.get()
		if used_src[req.src]==False and used_dest[req.dest]==False:
			used_src[req.src]=True
			used_dest[req.dest]=True
			
			req1 = Request(0, 0, 0, 0, 0, 0)
			req1.copy(req)
			#req1.size = 1

			admitted.put(req1)
			admitted_array[req1.src*(num_nodes+1)+req1.dest] = 1
			if flowlet:
				req2 = Request(0, 0, 0, 0, 0, 0)
				req2.copy(req)
				req2.size = req2.size - 1
				req2.ready = req2.ready + 1
				backlog.put(req2)

		else:
			backlog.put(req)

	return admitted, backlog, admitted_array


