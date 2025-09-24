def onShutdown():
	rabbitMQ.stop_consumer()
	#pass