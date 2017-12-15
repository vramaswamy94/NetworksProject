from gen import *
from Queue import *
from fastpass import Matching
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

    admittedQ, backlogQ = Matching(inp2FP, num_nodes, False)
   
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

    admittedQ, backlogQ = Matching(inp2FP, num_nodes, False)
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

def main():
    max_requests = 100
    num_nodes = 16 
    mean_arrival_rate = 20
    mean_size = 4 
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

if __name__=="__main__":
    main()


