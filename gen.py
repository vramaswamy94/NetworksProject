import sys
import random
from request import Request

##
# each packet record is of the form 
# (id, time_slot, 
##
class InputGen:
    
    def __init__(self, max_requests, num_nodes, mean_arrival_rate, mean_size, mean_prediction_delay):
        self.max_requests = max_requests
        self.num_nodes = num_nodes
        self.mean_arrival_rate = mean_arrival_rate
        self.mean_size = mean_size
        self.mean_prediction_delay = mean_prediction_delay
        self.current_time = 0
        self.id = 0
   
    def generate_next_request(self):
        inter_arrival_t = random.expovariate(self.mean_arrival_rate)
        src = random.randint(1, self.num_nodes)
        dest = src
        while src == dest:
            dest = random.randint(1, self.num_nodes)
        size = random.randint(1, 2*self.mean_size)
        self.current_time += inter_arrival_t
        self.id += 1
        prediction_delay = random.uniform(0, self.mean_prediction_delay*2)
        prediction_time = max(0, (self.current_time - prediction_delay))
        return (self.id, self.current_time, src, dest, size, prediction_time) 

    def generate_requests(self, timeslot):
        requests = []
        print "Generator params: arr_rate", self.mean_arrival_rate, "req_size", self.mean_size
        while self.id < self.max_requests:
            new_request = self.generate_next_request()
            for i in range(new_request[4]):
                nxt_packet = (new_request[0], new_request[1] + timeslot*i, new_request[2], new_request[3], i, new_request[4])
                requests.append(nxt_packet)
        return requests

    def check_stats(self, reqArray):
        last_arr_time = 0
        sum_interarrival_time = 0
        counts = {}
        sum_size = 0

        for i in range(self.num_nodes):
            counts[i+1] = 0

        for i in range(len(reqArray)):
            arr_time = reqArray[i][1]
            sum_interarrival_time += arr_time - last_arr_time
            last_arr_time = arr_time
            counts[reqArray[i][2]] += 1
            sum_size += reqArray[i][4]

        request_count = len(reqArray)
        
        print "Trace params: arr_rate", request_count/sum_interarrival_time, "req_size", sum_size/request_count
        print "Size", request_count

def convert_to_request_objects(reqArray):
    reqObjs = []
    for i in range(len(reqArray)):
        reqObjs.append(Request(reqArray[i][0], reqArray[i][2], reqArray[i][3], reqArray[i][4], reqArray[i][5], reqArray[i][1]))
    return reqObjs

def generate_input(max_requests, num_nodes, mean_arrival_rate, mean_size, mean_pred_delay, timeslot):
    gen = InputGen(max_requests, num_nodes, mean_arrival_rate, mean_size, mean_pred_delay)
    req_list = gen.generate_requests(timeslot)
    gen.check_stats(req_list)
    request_objs = convert_to_request_objects(req_list)
    return request_objs, req_list

