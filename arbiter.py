from gen import *
from Queue import *
from fastpass import Matching
import copy
from operator import itemgetter
import random
import numpy
random.seed(3453452)
numpy.random.seed(234231)

stats = {}

def main():
    num_nodes = 64
    max_requests = num_nodes*1000
    mean_arrival_rate = 150
    mean_pred_delay = 10
    timeslot = 1

    mean_size = 4 

    request_objs, request_list = generate_input(max_requests, num_nodes, mean_arrival_rate, mean_size, mean_pred_delay, timeslot)
    max_requests = len(request_list)

    print "baseline"
    init_stats(max_requests)
    arbiter(request_objs, timeslot, num_nodes) # Fastpass
    compute_wait_time()
    compute_flowlet_stats(request_list, num_nodes)

    fairness = "minmax"
    print fairness
    init_stats(max_requests)
    request_sorted_list = sorted(request_list, key=itemgetter(5), reverse=False)
    request_sorted_objs = convert_to_request_objects(request_sorted_list)
    priority_arbiter(request_sorted_objs, timeslot, num_nodes, fairness)
    compute_wait_time()
    compute_flowlet_stats(request_list, num_nodes)

    fairness = "reqtime"
    print fairness
    init_stats(max_requests)
    request_sorted_list = sorted(request_list, key=itemgetter(5), reverse=False)
    request_sorted_objs = convert_to_request_objects(request_sorted_list)
    priority_arbiter(request_sorted_objs, timeslot, num_nodes, fairness)
    compute_wait_time()
    compute_flowlet_stats(request_list, num_nodes)

    fairness = "sjf"
    print fairness
    init_stats(max_requests)
    request_sorted_list = sorted(request_list, key=itemgetter(5), reverse=False)
    request_sorted_objs = convert_to_request_objects(request_sorted_list)
    priority_arbiter(request_sorted_objs, timeslot, num_nodes, fairness)
    compute_wait_time()
    compute_flowlet_stats(request_list, num_nodes)


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
        stats[creq.id]["admitted_time"] = current_time + 4 * timeslot
        stats[creq.id]["ready_time"] = creq.ready
        stats[creq.id]["req_time"] = creq.req
        assert(stats[creq.id]["admitted_time"] >= stats[creq.id]["ready_time"])
    return backlogQ, saved_idx, admitted_queue_size

def priority_arbiter_iter(pQ, current_time, timeslot, num_nodes, inp2FP, fairness_array, fairness_policy):
    ready_list = []
    while not pQ.empty():
        queue_obj = pQ.get()
        creq = queue_obj[1]
        if creq.ready < current_time + 2 * timeslot:
            ready_list.append(creq)
        else:
            pQ.put(queue_obj)
            break
   
    if fairness_policy == "minmax":
        tmp_list = []
        for item in ready_list:
            tmp_list.append([item, fairness_array[item.src * (num_nodes + 1) + item.dest]])
        tmp_list_sorted = sorted(tmp_list, key=itemgetter(1), reverse=False)
        for i in range(len(tmp_list_sorted)):
            inp2FP.put(tmp_list_sorted[i][0])
    
    elif fairness_policy == "reqtime":
        tmp_list = []
        while not inp2FP.empty():
            creq = inp2FP.get()
            tmp_list.append([creq, creq.req])
        for item in ready_list:
            tmp_list.append([item, item.req])
        tmp_list_sorted = sorted(tmp_list, key=itemgetter(1), reverse=False)
        for i in range(len(tmp_list_sorted)):
            inp2FP.put(tmp_list_sorted[i][0])
    
    elif fairness_policy == "sjf":
        tmp_list = []
        while not inp2FP.empty():
            creq = inp2FP.get()
            tmp_list.append([creq, creq.flowlet_size])
        for item in ready_list:
            tmp_list.append([item, item.flowlet_size])
        tmp_list_sorted = sorted(tmp_list, key=itemgetter(1), reverse=False)
        for i in range(len(tmp_list_sorted)):
            inp2FP.put(tmp_list_sorted[i][0])
        
    admittedQ, backlogQ = Matching(inp2FP, num_nodes, False)
    admitted_queue_size = admittedQ.qsize()

    while not admittedQ.empty():
        creq = admittedQ.get()
        stats[creq.id]["admitted_time"] = current_time + 3 * timeslot
        stats[creq.id]["ready_time"] = creq.ready
        stats[creq.id]["req_time"] = creq.req
        assert(stats[creq.id]["admitted_time"] >= stats[creq.id]["ready_time"])

        if fairness_policy == "minmax":
            fairness_array[creq.src * (num_nodes+1) + creq.dest] = current_time 


    return backlogQ, pQ, admitted_queue_size, fairness_array

def priority_arbiter(request_objs, timeslot, num_nodes, fairness_policy):
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
    backlogQ, pQ, admitted_queue_size, fairness_array = priority_arbiter_iter(pQ, current_time, timeslot, num_nodes, Queue(), fairness_array, fairness_policy)
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
        backlogQ, pQ, admitted_queue_size, fairness_array = priority_arbiter_iter(pQ, current_time, timeslot, num_nodes, backlogQ, fairness_array, fairness_policy)
        current_time += timeslot
        sum_admitted_cnt += admitted_queue_size
        iter_cnt += 1
    
    print "Iterations:", iter_cnt 
    assert(sum_admitted_cnt == len(request_objs))

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

def gen_3d_stats_array(x, y, z, val):
    arr = {}
    for i in range(x):
        arr[i] = {}
        for j in range(y):
            arr[i][j] = {}
            for k in range(z):
                arr[i][j][k] = val
    return arr

def compute_flowlet_stats(request_list, num_nodes):
    on_periods = request_list[-1][6]/2 + 1
    fl_first_pkt_ready_time = gen_3d_stats_array(num_nodes, num_nodes, on_periods, float('inf')) 
    fl_last_pkt_admitted_time = gen_3d_stats_array(num_nodes, num_nodes, on_periods, -float('inf')) 
    fl_wait_time = gen_3d_stats_array(num_nodes, num_nodes, on_periods, -1)
    
    for i in stats.keys():
        reqid = i
        pckt = request_list[reqid-1]
        assert(pckt[0] == reqid)
        src = pckt[2]-1
        dest = pckt[3]-1
        period = pckt[6]/2

        fl_first_pkt_ready_time[src][dest][period] = min(fl_first_pkt_ready_time[src][dest][period], stats[i]["ready_time"])
        fl_last_pkt_admitted_time[src][dest][period]  = max(fl_last_pkt_admitted_time[src][dest][period], stats[i]["admitted_time"])
        fl_wait_time[src][dest][period] = fl_last_pkt_admitted_time[src][dest][period] - fl_first_pkt_ready_time[src][dest][period]

    tmp = []
    for i in range(num_nodes):
        for j in range(num_nodes):
            for k in range(on_periods):
                if fl_wait_time[i][j][k] != -1:
                    tmp.append(fl_wait_time[i][j][k])
    
    print "Flowlet Transmission Time: Min", "%0.2f" % min(tmp), "Avg.", "%0.2f" % numpy.mean(tmp), "Max", "%0.2f" % max(tmp)
def print_queue(q):
    while not q.empty():
        item = q.get()
        item.print_request()

def vary_arrival_rate():
    for num_nodes in [16,32,64,128,256]:
        max_requests = num_nodes*500
        mean_pred_delay = 10
        timeslot = 1
        
        mean_size = 1           #not used

        out_file = "AvgWaitTime_Poisson_" + str(num_nodes)
        f = open(out_file, "w")
        for arrival_factor in range(1,6):
            
            mean_arrival_rate = arrival_factor*num_nodes
            request_objs, request_list = generate_input(max_requests, num_nodes, mean_arrival_rate, mean_size, mean_pred_delay, timeslot)
            max_requests = len(request_list)
            init_stats(max_requests)
            arbiter(request_objs, timeslot, num_nodes) # Fastpass
            fp_avg, fp_max, fp_min = compute_wait_time()

            init_stats(max_requests)
            request_sorted_list = sorted(request_list, key=itemgetter(5), reverse=False)
            request_sorted_objs = convert_to_request_objects(request_sorted_list)
            priority_arbiter(request_sorted_objs, timeslot, num_nodes, "minmax")
            req_avg, req_max, req_min = compute_wait_time()
            f.write(str(mean_arrival_rate) + "\t" +  str(fp_min) + "\t" + str(req_min) + "\t"+ str(fp_avg) + "\t" + str(req_avg)+"\t"+  str(fp_max) + "\t" + str(req_max)+ "\n")

        f.close()

if __name__=="__main__":
    main()


