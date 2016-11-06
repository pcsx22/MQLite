import sys,json,socket,time


class Producer:
	def __init__(self,topic,server_address):
		self.topic = topic
		self.server_address = server_address
		self.sock = self.get_socket()

	def get_socket(self):
		sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
		return sock

	def start(self):
		while True:
			self.sock.sendto(json.dumps({'topic':"hello",'message':"Ramailo Jind.."}),self.server_address)
			#self.sock.sendto("Hello")
			print "sent to server...."
			time.sleep(5)

P = Producer("hello",('127.0.0.1',2556))
P.start()