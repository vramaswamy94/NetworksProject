import sys
import random
from request import Request
import numpy
import math

##
# each packet record is of the form 
# (id, time_slot, 
##

ON_PERIOD_FACTOR = 5000
OFF_PERIOD_FACTOR = 10

class InputGen:
    
    def __init__(self, max_requests, num_nodes, mean_arrival_rate, mean_size, mean_prediction_delay, interarrival_distribution):
        self.max_requests = max_requests
        self.num_nodes = num_nodes
        self.mean_arrival_rate = mean_arrival_rate
        self.mean_size = mean_size
        self.mean_prediction_delay = mean_prediction_delay
        self.current_time = 0
        self.id = 0
        self.interarrival_distribution = interarrival_distribution
        self.req_iat = 1.0/self.mean_arrival_rate

        if self.interarrival_distribution == "exponential":
            self.iats_list = numpy.random.exponential(self.req_iat, max_requests)
        else:
            self.iats_list = numpy.random.lognormal(math.log(self.req_iat), 1, max_requests)
       
        self.on_period_durations = numpy.random.lognormal(math.log(self.req_iat*ON_PERIOD_FACTOR), 1, max_requests)
        self.off_period_durations = numpy.random.lognormal(math.log(self.req_iat*OFF_PERIOD_FACTOR), 1, max_requests)
        # generating max_requests on/off periods is overkill

        self.on_off_durations = []
        for i in range(max_requests):
            self.on_off_durations.append(self.on_period_durations[i])
            self.on_off_durations.append(self.off_period_durations[i])
        self.on_off_times = numpy.cumsum(self.on_off_durations)
        self.period_idx = 0

        self.flowlet_request_time = {}
        for i in range(num_nodes+1):
            self.flowlet_request_time[i] = {}
            for j in range(num_nodes+1):
                self.flowlet_request_time[i][j] = -1
       
        self.init_flowlet_stats()
    
    def init_flowlet_stats(self):
        self.flowlet_length = {}
        self.acc_flowlet_length = {}
        self.flowlet_count = {}
        self.flowlet_packet_ids = {}
        for i in range(self.num_nodes+1):
            self.flowlet_length[i] = {}
            self.acc_flowlet_length[i] = {}
            self.flowlet_count[i] = {}
            self.flowlet_packet_ids[i] = {}
            for j in range(self.num_nodes+1):
                self.flowlet_length[i][j] = 0
                self.acc_flowlet_length[i][j] = 0
                self.flowlet_count[i][j] = 0
                self.flowlet_packet_ids[i][j] = []

    def reset_flowlet_request_times(self):
        for i in range(self.num_nodes+1):
            for j in range(self.num_nodes+1):
                self.flowlet_request_time[i][j] = -1

    def acc_reset_flowlet_length_counter(self):
        for i in range(self.num_nodes+1):
            for j in range(self.num_nodes+1):
                if self.flowlet_length[i][j] != 0:
                    for ident in self.flowlet_packet_ids[i][j]:
                        assert(self.requests[ident-1][0] == ident)
                        self.requests[ident-1].append(self.flowlet_length[i][j])

                    self.acc_flowlet_length[i][j] += self.flowlet_length[i][j]
                    self.flowlet_count[i][j] += 1
                    self.flowlet_length[i][j] = 0
                    self.flowlet_packet_ids[i][j] = []

    def generate_next_request(self, last_packet_flag):
        # Always operate in on periods
        assert(self.period_idx % 2 == 0)

        inter_arrival_t = self.iats_list[self.id]
        src = random.randint(1, self.num_nodes)
        dest = src
        while src == dest:
            dest = random.randint(1, self.num_nodes)
        size = random.randint(1, 2*self.mean_size)

        jump_time = 0
        if self.current_time + inter_arrival_t > self.on_off_times[self.period_idx]:
            # End of on period
            # Truncate this packet's arrival time to occur at the end of the on period
            inter_arrival_t = self.on_off_times[self.period_idx]-self.current_time 
            # To jump by the length of the upcoming off period
            jump_time = self.on_off_times[self.period_idx+1]  
            
        self.current_time += inter_arrival_t
        self.id += 1
        if self.flowlet_request_time[src][dest] != -1:
            prediction_time = self.flowlet_request_time[src][dest]
        else:
            prediction_delay = random.uniform(0, self.mean_prediction_delay*2)
            prediction_time = max(0, (self.current_time - prediction_delay))
            self.flowlet_request_time[src][dest] = prediction_time

        self.flowlet_length[src][dest] += 1
        self.flowlet_packet_ids[src][dest].append(self.id)

        pckt = [self.id, self.current_time, src, dest, 1, prediction_time, self.period_idx]

        flag = False
        if jump_time != 0 or last_packet_flag:
            flag = True
            self.current_time = jump_time
            # Setting the id to the next on period
            self.period_idx += 2
        return pckt, flag

    def generate_requests(self, timeslot):
        self.requests = []
        print "Generator params: arr_rate:", self.mean_arrival_rate, "num_nodes:", self.num_nodes
        while self.id < self.max_requests:
            last_packet_flag = (self.id == self.max_requests-1)
            new_request, ret_flag = self.generate_next_request(last_packet_flag)
            assert(new_request[4] == 1)
            self.requests.append(new_request)
            if ret_flag:
                self.reset_flowlet_request_times()
                self.acc_reset_flowlet_length_counter()
        return self.requests

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
        
        flowlet_length = []
        total_count = 0
        for i in range(self.num_nodes+1):
            for j in range(self.num_nodes+1):
                if self.flowlet_count[i][j] != 0:
                    #print "src:", i, "dest:", j, "packets:", self.acc_flowlet_length[i][j], "active_periods:", self.flowlet_count[i][j], "avg. flowlet length:", self.acc_flowlet_length[i][j]/self.flowlet_count[i][j]
                    total_count += self.acc_flowlet_length[i][j]
                    self.acc_flowlet_length[i][j] /= self.flowlet_count[i][j]
                    flowlet_length.append(self.acc_flowlet_length[i][j])
        print total_count 
        assert(total_count == len(reqArray))
        print "Trace params: arr_rate", request_count/sum_interarrival_time, "total_packets", request_count
        print "Average flowlet length:", numpy.mean(flowlet_length)
        print "Number of on periods:", self.period_idx/2
        print "Average packets per on period:", len(reqArray)/(self.period_idx/2)

def convert_to_request_objects(reqArray):
    reqObjs = []
    for i in range(len(reqArray)):
        reqObjs.append(Request(reqArray[i][0], reqArray[i][2], reqArray[i][3], reqArray[i][4], reqArray[i][5], reqArray[i][1]))
    return reqObjs

def generate_input(max_requests, num_nodes, mean_arrival_rate, mean_size, mean_pred_delay, timeslot):
    gen = InputGen(max_requests, num_nodes, mean_arrival_rate, mean_size, mean_pred_delay, "lognormal")
    req_list = gen.generate_requests(timeslot)
    #for req in req_list:
    #    print req
    gen.check_stats(req_list)
    request_objs = convert_to_request_objects(req_list)
    return request_objs, req_list

