import sys,socket,time,simplejson
import cPickle as pickle
from random import randint
from router import Router
#consumer listens on a randomly generated port(i.e self.get_port() and notifies broker about it) and receives message along with 
#other consumer's ip where it needs to send the message
 
class Consumer:
	def __init__(self,topic,server_address):
		self.topic = topic
		self.server_address = server_address
		self.sock = self.get_socket()
		self.listen_addr = (server_address[0],self.get_port())
		self.sock.bind(self.listen_addr)
		self.router = Router(self.sock)

	def get_socket(self):
		sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
		return sock

	def get_port(self):
		return randint(10000,20000)

	def register(self):
		#send port number on which the consumer will listen to 
		self.sock.sendto(simplejson.dumps((self.listen_addr[1],self.topic)),self.server_address)

	def start(self):
		self.register()
		while True:
			print "Waiting for message from server...\n"
			data,address = self.sock.recvfrom(4096)
			if len(data) > 1:
				data = simplejson.loads(data)
				print data
				if data[0] == 0: 
					self.router.route_cons(data[1],data[2])
					print "la ayo hai..."

C = Consumer("hello",('127.0.0.1',2555))
C.start()

	

