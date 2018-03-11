<h2>MQLite</h2>
A basic P2P implementation of publisher subscriber pattern of message passing where every subscriber is also publisher. A publisher doesn't
publish data on a topic to every subscriber, infact the subscribers which directly receives data from the publisher passes data to other
subscriber which continues until every subscribe receives data. The main publisher publishes data belonging to a topic along with routing information for the subscriber so that they can help exchanging the message to others. 
