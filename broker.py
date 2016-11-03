import sys,socket,time,json,Queue
from threading import Thread

#OPTIMISE: BROADCAST/MULTICAST
#OPTIMISE: the socket which is used to send data to consumer is used heavily by all the threads
#ADD WEBSOCKET FUNCTIONALITY

CONSUMER_ACCEPTOR_PORT = 2555
PRODUCER_PORT = 2556
consumer_dict = {}
message_queues = {}
pipeline = Queue.Queue() #Blocking Queue which will be used by Dispatcher and Handler thread
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

	#send all the existing message on topic
	def msg_init(self,topic,address): #this implementation is very naive, threads should be synchronised while using message_queues[topic]
		q = message_queues[topic]
		if not q.get(q.next) is None:
			for i in range(q.next,q.length):
				self.sock.sendto(json.dumps(q.get(i)),address)
		for i in range(0,q.next):
			self.sock.sendto(json.dumps(q.get(i)),address)
			

	def run(self):
		print "Thread ConsumerAcceptor running..."
		while True:
			data,address = self.sock.recvfrom(4096)
			data = json.loads(data)
			topic = data[1]
			if topic not in consumer_dict: 
				consumer_dict[topic] = list()
			
			consumer_dict[topic].append(Consumer(int(time.time()),topic,address))
			if topic not in message_queues:
				message_queues[topic] = MessageQueue(20)
			else:
				self.sock.sendto("Registered for topic: " + topic,address)
				self.msg_init(topic,address)
			
			print "Consumer joined for the first time..."

class Handler(Thread): #handles publishers
	
	def __init__(self):
		Thread.__init__(self)
		self.sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
		self.sock.bind(('127.0.0.1',PRODUCER_PORT))

	def run(self):
		print "Thread Handler running..."
		while True:
			data,address = self.sock.recvfrom(4096)
			data = json.loads(data)
			topic = data['topic']
			if topic not in message_queues:
				message_queues[topic] = MessageQueue(20)
			
			message_queues[topic].insert(data) #store the data in message queue for that topic
			pipeline.put(data) #dispatching will be done by dispatcher thread
			print "FROM HANDLER THREAD -> Data Received.."

class Dispatcher(Thread):

	def __init__(self,cons_sock):
		Thread.__init__(self)
		self.sock = cons_sock

	def broadcast(self,msg,topic):
		map(lambda x: self.sock.sendto(msg,x.address),consumer_dict[topic])
		return True

	def run(self):
		while True:
			try:
				data = pipeline.get(False)
				#this needs optimisation[Routing]
				data['topic'] in consumer_dict and self.broadcast(json.dumps(data),data['topic'])
				print "FROM DISPATCHER THREAD -> Dispatched"
			except Queue.Empty:
				time.sleep(5)

c = ConsumerAcceptor()
c.daemon = True
c.start()

h = Handler()
h.daemon = True
h.start()

d = Dispatcher(c.sock)
d.daemon = True
d.start()

while True:
	time.sleep(10)