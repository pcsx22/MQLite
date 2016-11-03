import sys,socket,json

class Message:
	def __init__(self,topic,message):
		self.topic,self.message = topic,message

class Consumer:
	def __init__(self,topic,server_address):
		self.topic = topic
		self.server_address = server_address
		self.sock = self.get_socket()

	def get_socket(self):
		sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
		return sock

	def register(self):
		self.sock.sendto(json.dumps((1,self.topic)),self.server_address)

	def start(self):
		self.register()
		while True:
			print "Waiting for message from server...\n"
			data,address = self.sock.recvfrom(4096)
			print "Received %s" % data

C = Consumer("hello",('127.0.0.1',2555))
C.start()

	

