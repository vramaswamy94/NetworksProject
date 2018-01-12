from Queue import *
from request import *


def predictor_core(usage, scale, num_nodes, interval):
	demand = [0] * (num_nodes +1) * (num_nodes+1)
	for i in range(1,num_nodes + 1):
		for j in range(1,num_nodes + 1):
			if (usage[i*(num_nodes+1) + j] < 1):
				demand[i*(num_nodes + 1) + j] = 1 # assuming min demand is 1 in n timeslots
			#elif (usage[i*(num_nodes + 1) + j] * scale > interval):
			#	demand[i*(num_nodes+1)+j]= interval # max is n in n time slots
			else:
				demand[i*(num_nodes+1)+j] = int(usage[i*(num_nodes+1)+j]*scale) 
	
	return demand

def make_request_queue(predicted_demand, current_time, num_nodes):
	requestQ = Queue()
	for i in range(num_nodes + 1):
		for j in range(num_nodes + 1):
			for k in range(predicted_demand[i*(num_nodes+1) + j]):
				requestQ.put(Request(0,i, j, 1, current_time, current_time, 1))
	return requestQ


#usage_array=[0]*11*11
#for i in range(1,11):
#	for j in range(1,11):
#		usage_array[11*i + j] = (i+j)%10

#scale = 1.2
#num_nodes = 10
#interval = 10

#demand = predictor_core(usage_array, scale, num_nodes, interval)
#actual_demand = [0]*121
#req_list = make_request_list(demand, 0, actual_demand, 10)
#for i in range(len(req_list)):
#	req_list[i].print_request()




