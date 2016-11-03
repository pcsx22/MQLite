#Router class handles the routing mechanism for the the broker
#the idea is to split the consumer into 2 halves and pass the two halves to alive consumer, this process of halving down the 
#consumer continues

class Router:
	def __init__(sock):
		self.sock = sock

	def route(msg,topic):
		consumer_list = consumer_dict[topic]
		l_list,r_list = consumer_list[1:len(consumer_list)/2],consumer_list[len(consumer_dict)/2 + 1:len(consumer_list)]
		l_target,r_target = l_list.pop(0),r_list.pop(0)
		#check if these targets are alive
		self.sock.sendto(json.dumps((msg,l_list)),l_target.address)
		self.sock.sendto(json.dumps(msg,r_list,r_target.address)