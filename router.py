#Router class handles the routing mechanism for the the broker
#the idea is to split the consumer into 2 halves and pass the two halves to alive consumer, this process of halving down the 
#consumer continues
import json
class Router:
	def __init__(self,sock):
		self.sock = sock
	#this doesnt implement the splitting implementation which I have mentioned above in my commnet.
	#as of now, n-1 consumer's address are sent to a random consumer and this  routing continues until no consumer is left in the list
	def route(self,msg,cons_list):
		topic = msg['topic']
		consumer_list = [x.address for x in cons_list]
		len(consumer_list) > 1 and self.sock.sendto(json.dumps((0,msg,consumer_list[1:])),consumer_list[0])
		len(consumer_list) == 1 and self.sock.sendto(json.dumps((1,msg)),consumer_list[0])
		print "--> From Router"

	def route_cons(self,msg,cons_list):
		len(cons_list) > 1 and self.sock.sendto(json.dumps((0,msg,cons_list[1:])),tuple(cons_list[0]))
		len(cons_list) == 1 and self.sock.sendto(json.dumps((1,msg)),tuple(cons_list[0]))
		print "----> From Router"