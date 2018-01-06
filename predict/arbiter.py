from gen import *
from Queue import *
from fastpass import Matching
from predict import *
import copy
from operator import itemgetter

stats = {}

def init_stats(max_requests):
    for i in range(max_requests):
        stats[i+1] = {}

def compute_wait_time():
    waits = []
    for i in stats.keys():
        waits.append(stats[i]["admitted_time"] - stats[i]["ready_time"])
    average_wait = sum(waits)/len(stats.keys())
    max_wait = max(waits)
    min_wait = min(waits)
    print "Wait Time: Min", "%0.2f" % min_wait, "Avg.", "%0.2f" % average_wait, "Max", "%0.2f" % max_wait

def print_queue(q):
    while not q.empty():
        item = q.get()
        item.print_request()


def arbiter(request_objs, timeslot, num_nodes):
    backlogQ, saved_idx, sum_admitted_cnt = arbiter_iter(request_objs, 0, timeslot, num_nodes, 0, Queue())
    current_time = timeslot
    iter_cnt = 1
    while (not backlogQ.empty()) or (saved_idx < len(request_objs)):
        backlogQ, saved_idx, admitted_cnt = arbiter_iter(request_objs, current_time, timeslot, num_nodes, saved_idx, backlogQ)
        current_time += timeslot
        sum_admitted_cnt += admitted_cnt
        iter_cnt += 1
    print "Iterations:", iter_cnt
    assert(sum_admitted_cnt == len(request_objs))

def arbiter_iter(input_requests, current_time, timeslot, num_nodes, saved_idx, inp2FP):
    new_req_cnt = 0
    i = saved_idx 
    while i < len(input_requests):
        req = input_requests[i]
        if req.ready < current_time + timeslot:
            inp2FP.put(req)
            new_req_cnt += 1
        else:
            break
        i += 1
    saved_idx = i

    admittedQ, backlogQ, admitted_array = Matching(inp2FP, num_nodes, False)
   
    admitted_queue_size = admittedQ.qsize()
    while not admittedQ.empty():
        creq = admittedQ.get()
        stats[creq.id]["admitted_time"] = current_time + timeslot
        stats[creq.id]["ready_time"] = creq.ready
        stats[creq.id]["req_time"] = creq.req
        assert(stats[creq.id]["admitted_time"] >= stats[creq.id]["ready_time"])
    return backlogQ, saved_idx, admitted_queue_size

def priority_arbiter_iter(pQ, current_time, timeslot, num_nodes, inp2FP, fairness_array):
    tmp_list = []
    while not pQ.empty():
        queue_obj = pQ.get()
        creq = queue_obj[1]
        if creq.ready < current_time + timeslot:
            tmp_list.append([creq, fairness_array[creq.src * (num_nodes+1) + creq.dest]])
        else:
            pQ.put(queue_obj)
            break
   
    tmp_list_sorted = sorted(tmp_list, key=itemgetter(1), reverse=False)
    for i in range(len(tmp_list_sorted)):
        inp2FP.put(tmp_list_sorted[i][0])

    admittedQ, backlogQ, admitted_array = Matching(inp2FP, num_nodes, False)
    admitted_queue_size = admittedQ.qsize()

    while not admittedQ.empty():
        creq = admittedQ.get()
        fairness_array[creq.src * (num_nodes+1) + creq.dest] = current_time 

    return backlogQ, pQ, admitted_queue_size, fairness_array

def priority_arbiter(request_objs, timeslot, num_nodes):
    pQ = PriorityQueue()
    saved_idx = 0
    current_time = 0
    sum_admitted_cnt = 0
    
    fairness_array = [0]*(num_nodes+1)*(num_nodes+1)
     
    while saved_idx < len(request_objs):
        if request_objs[saved_idx].req < current_time + timeslot:
            pQ.put((request_objs[saved_idx].ready, request_objs[saved_idx]))
            saved_idx += 1
        else:
            break
    backlogQ, pQ, admitted_queue_size, fairness_array = priority_arbiter_iter(pQ, current_time, timeslot, num_nodes, Queue(), fairness_array)
    current_time += timeslot
    sum_admitted_cnt += admitted_queue_size
    
    iter_cnt = 1
    while (not backlogQ.empty()) or (saved_idx < len(request_objs)) or (not pQ.empty()):
        while saved_idx < len(request_objs):
            if request_objs[saved_idx].req < current_time + timeslot:
                pQ.put((request_objs[saved_idx].ready, request_objs[saved_idx]))
                saved_idx += 1
            else:
                break
        backlogQ, pQ, admitted_queue_size, fairness_array = priority_arbiter_iter(pQ, current_time, timeslot, num_nodes, backlogQ, fairness_array)
        current_time += timeslot
        sum_admitted_cnt += admitted_queue_size
        iter_cnt += 1
    
    print "Iterations:", iter_cnt 
    assert(sum_admitted_cnt == len(request_objs))

def predict_arbiter_iter(backlog_predictQ, backlog_actualQ, usage, fairness_array, timeslot, current_time, num_nodes, saved_idx, request_objs, admitted_array, pred_num):
	new_req = []
	sorted_new_req= []
	i = saved_idx 
	if not current_time==0:
		while i < len(request_objs):
			req = request_objs[i]
			if req.ready <= current_time - timeslot:
				if (admitted_array[req.src*(num_nodes+1)+req.dest]==0):
					new_req.append([req, fairness_array[req.src*(num_nodes+1)+req.dest]])
				else:
					pred_num+=1
				usage[req.src*(num_nodes+1)+req.dest]+=1
				#if pred_demand[req.src*(num_nodes+1)+req.dest] > 0:
				#	pred_demand[req.src*(num_nodes+1)+ req.dest] -=1
			else:
				break
			i += 1
		saved_idx = i
		sorted_new_req = sorted(new_req, key=itemgetter(1), reverse=False)
		
	pred_demand_temp = []
	while not backlog_predictQ.empty():
		req = backlog_predictQ.get()
		pred_demand_temp.append([req,fairness_array[req.src*(num_nodes+1)+ req.dest]] )
		
	sorted_pred_req = sorted(pred_demand_temp, key=itemgetter(1), reverse=False)

	inp2FP = Queue()
	while not backlog_actualQ.empty():
		inp2FP.put(backlog_actualQ.get())
	for i in range(len(sorted_new_req)):
		inp2FP.put(sorted_new_req[i][0])
	for i in range(len(sorted_pred_req)):
		inp2FP.put(sorted_pred_req[i][0])

	admittedQ, backlogQ, admitted_array = Matching(inp2FP, num_nodes, 0)
		
	admitted_queue_size = admittedQ.qsize()
	while not admittedQ.empty():
		creq = admittedQ.get()
		fairness_array[creq.src * (num_nodes+1) + creq.dest] = current_time + timeslot

	backlog_actualQ = Queue()
	backlog_predictQ = Queue()
	while not backlogQ.empty():
		req = backlogQ.get()
		if req.predict == 1:
			backlog_predictQ.put(req)
		else:
			backlog_actualQ.put(req)
	return backlog_predictQ, backlog_actualQ, admitted_array, admitted_queue_size, saved_idx, fairness_array, usage, pred_num

def predict_arbiter(request_objs, timeslot, num_nodes, interval, scale):
	current_time=0
	fairness_array = [0]*(num_nodes+1)*(num_nodes+1)
	usage= [0] *(num_nodes+1)*(num_nodes+1)
	pred_demand = predictor_core(usage, scale, num_nodes, interval)
	predQ = make_request_queue(pred_demand, current_time, num_nodes)
	pred_num=0
	#print predQ.qsize(), pred_demand[12]
	
	backlog_predictQ, backlog_actualQ, admit_array, admit_size, saved_idx, fairness_array, usage, pred_num = predict_arbiter_iter(predQ, Queue(), usage, fairness_array, timeslot, current_time, num_nodes,  0, request_objs, [0]*(num_nodes+1)*(num_nodes+1), pred_num )

	#print backlog_predictQ.qsize(), backlog_actualQ.qsize(), admit_size
	
	iter_cnt = 1
	current_time = 1
	while True:
		if iter_cnt%interval ==0:
			pred_demand = predictor_core(usage, scale, num_nodes, interval)
			backlog_predictQ = make_request_queue(pred_demand, current_time, num_nodes)
		
		backlog_predictQ, backlog_actualQ, admit_array, admit_size, saved_idx, fairness_array, usage, pred_num = predict_arbiter_iter(backlog_predictQ, backlog_actualQ, usage, fairness_array, timeslot, current_time, num_nodes, saved_idx, request_objs, admit_array, pred_num)
		#print backlog_predictQ.qsize(), backlog_actualQ.qsize(), admit_size
		
		iter_cnt+=1
		current_time+=1
		if (backlog_actualQ.empty() and saved_idx== len(request_objs)):
			break


	print "Iterations: ", iter_cnt
	print "PredictedL ", pred_num	


def main():
    max_requests = 100
    num_nodes = 16
    mean_arrival_rate = 20
    mean_size = 1 
    mean_pred_delay = 10
    timeslot = 1

    request_objs, request_list = generate_input(max_requests, num_nodes, mean_arrival_rate, mean_size, mean_pred_delay, timeslot)
    max_requests = len(request_list)
    init_stats(max_requests)
    arbiter(request_objs, timeslot, num_nodes)
    #compute_wait_time()

    request_sorted_list = sorted(request_list, key=itemgetter(5), reverse=False)
    request_sorted_objs = convert_to_request_objects(request_sorted_list)
    priority_arbiter(request_sorted_objs, timeslot, num_nodes)

    predict_arbiter(request_objs, timeslot,num_nodes, 10, 1.5)

if __name__=="__main__":
    main()

