import sys
import random
from request import Request
import numpy
import math

##
# each packet record is of the form 
# (id, time_slot, 
##
class InputGen:
    
    def __init__(self, max_requests, num_nodes, mean_arrival_rate, mean_size, mean_prediction_delay, interarrival_distribution, flowlet_mode):
        self.max_requests = max_requests
        self.num_nodes = num_nodes
        self.mean_arrival_rate = mean_arrival_rate
        self.mean_size = mean_size
        self.mean_prediction_delay = mean_prediction_delay
        self.current_time = 0
        self.id = 0
        self.interarrival_distribution = interarrival_distribution
        self.flowlet_mode = flowlet_mode
        self.req_iat = 1.0/self.mean_arrival_rate

        if self.interarrival_distribution == "exponential":
            self.iats_list = numpy.random.exponential(self.req_iat, max_requests)
        else:
            self.iats_list = numpy.random.lognormal(math.log(self.req_iat), 1, max_requests)
        
        self.on_period_durations = numpy.random.lognormal(math.log(self.req_iat*10), 1, max_requests)
        self.off_period_durations = numpy.random.lognormal(math.log(self.req_iat*10), 1, max_requests)
        self.on_off_durations = []
        for i in range(max_requests):
            self.on_off_durations.append(self.on_period_durations[i])
            self.on_off_durations.append(self.off_period_durations[i])
        self.on_off_times = numpy.cumsum(self.on_off_durations)
        self.period_idx = 0

    def generate_next_request(self):
        assert(self.period_idx % 2 == 0)

        inter_arrival_t = self.iats_list[self.id]
        src = random.randint(1, self.num_nodes)
        dest = src
        while src == dest:
            dest = random.randint(1, self.num_nodes)
        size = random.randint(1, 2*self.mean_size)

        jump_time = 0
        if self.current_time + inter_arrival_t > self.on_off_times[self.period_idx]:
            #print "new slot at packet", self.id, self.current_time, self.current_time+inter_arrival_t, self.on_off_times[self.period_idx]
            #end of on period
            inter_arrival_t = self.on_off_times[self.period_idx]-self.current_time
            jump_time = self.on_off_times[self.period_idx+1]
            self.period_idx += 2

        self.current_time += inter_arrival_t
        self.id += 1
        prediction_delay = random.uniform(0, self.mean_prediction_delay*2)
        prediction_time = max(0, (self.current_time - prediction_delay))
        if self.flowlet_mode:
            pckt = (self.id, self.current_time, src, dest, size, prediction_time) 
        else:
            pckt = (self.id, self.current_time, src, dest, 1, prediction_time) 

        if jump_time != 0:
            self.current_time = jump_time

        return pckt

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
    gen = InputGen(max_requests, num_nodes, mean_arrival_rate, mean_size, mean_pred_delay, "lognormal", False)
    req_list = gen.generate_requests(timeslot)
    gen.check_stats(req_list)
    request_objs = convert_to_request_objects(req_list)
    return request_objs, req_list

