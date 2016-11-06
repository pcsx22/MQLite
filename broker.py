import sys,socket,time,json,Queue,simplejson
from threading import Thread
from router import Router
import cPickle as pickle
#OPTIMISE: BROADCAST/MULTICAST
#OPTIMISE: the socket which is used to send data to consumer is used heavily by all the threads
#ADD WEBSOCKET FUNCTIONALITY

CONSUMER_ACCEPTOR_PORT = 2555
PRODUCER_PORT = 2556
consumer_dict = {}
message_queues = {}

class Mediator:
	def __init__(self):
		self.pipeline = Queue.Queue() #Blocking Queue which will be used by Dispatcher and Handler thread

	def send_to_dispatcher(self,message):
		self.pipeline.put(message)

	def receive_from_handler(self):
		try:
			message = self.pipeline.get(False)
			return message
		except Queue.Empty:
			return None

class Consumer:
	
	def __init__(self,id,topic,address):
		self.id = id
		self.topic = topic
		self.address = address
		self.last_index = 0

class MessageQueue:

	def __init__(self,length):
		self.length,self.next = length,0
		self.container = [None] * length #creates empty list of size length

	def insert(self,msg):
		self.container[self.next] = msg
		self.next += 1
		self.next %= self.length

	def get(self,index):
		return self.container[index]

class ConsumerAcceptor(Thread):
	
	def __init__(self):
		Thread.__init__(self)
		self.sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
		self.sock.bind(('127.0.0.1',CONSUMER_ACCEPTOR_PORT))
		self.cons_sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
	#send all the existing message on topic
	def msg_init(self,topic,address): #this implementation is very naive, threads should be synchronised while using message_queues[topic]
		q = message_queues[topic]
		if not q.get(q.next) is None:
			for i in range(q.next,q.length):
				self.sock.sendto(json.dumps((1,q.get(i))),address)
		for i in range(0,q.next):
			self.sock.sendto(json.dumps((1,q.get(i))),address)
	

	def run(self):
		print "Thread ConsumerAcceptor running..."
		while True:
			data,address = self.sock.recvfrom(4096)
			data = json.loads(data)
			topic = data[1]
			address = (address[0],data[0])
			print address
			if topic not in consumer_dict: 
				consumer_dict[topic] = list()
			
			consumer_dict[topic].append(Consumer(int(time.time()),topic,address))
			if topic not in message_queues:
				message_queues[topic] = MessageQueue(20)
			else:
				#self.sock.sendto("Registered for topic: " + topic,address)
				self.msg_init(topic,address)
			
			print "Consumer joined for the first time..."

class Handler(Thread): #handles publishers
	
	def __init__(self,mediator):
		Thread.__init__(self)
		self.sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
		self.sock.bind(('127.0.0.1',PRODUCER_PORT))
		self.mediator = mediator

	def run(self):
		print "Thread Handler running..."
		while True:
			data,address = self.sock.recvfrom(4096)
			data = json.loads(data)
			topic = data['topic']
			if topic not in message_queues:
				message_queues[topic] = MessageQueue(20)
			message_queues[topic].insert(data) #store the data in message queue for that topic
			self.mediator.send_to_dispatcher(data) #dispatching will be done by dispatcher thread
			print "FROM HANDLER THREAD -> Data Received.."

class Dispatcher(Thread):

	def __init__(self,cons_sock,mediator):
		Thread.__init__(self)
		self.sock = cons_sock
		self.r = Router(self.sock)
		self.mediator = mediator

	def broadcast(self,msg,topic):
		map(lambda x: self.sock.sendto(msg,x.address),consumer_dict[topic])
		return True

	def run(self):
		while True:
			data = self.mediator.receive_from_handler()
			if data is not None:
				data['topic'] in consumer_dict and self.r.route(data,consumer_dict[data['topic']])
				print "FROM DISPATCHER THREAD -> Dispatched"
			else:
				time.sleep(5)

c = ConsumerAcceptor()
c.daemon = True
c.start()
mediator = Mediator()

h = Handler(mediator)
h.daemon = True
h.start()

d = Dispatcher(c.sock,mediator)
d.daemon = True
d.start()

while True:
	time.sleep(10)