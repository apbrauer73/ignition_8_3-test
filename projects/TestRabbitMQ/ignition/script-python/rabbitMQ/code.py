# Gateway Event Script - Application Startup Event
import threading
import time
from java.lang import String
from java.lang import Runnable, Thread
from java.util.concurrent import Executors
from com.rabbitmq.client import ConnectionFactory, Connection, Channel, Consumer, DefaultConsumer, Envelope
from com.rabbitmq.client.AMQP import BasicProperties

class RabbitMQConsumer(Runnable):
	def __init__(self, queue_name, tag_path_prefix="[rabbitMQData]MQData/"):
		self.queue_name = queue_name
		self.tag_path_prefix = tag_path_prefix
		self.connection = None
		self.channel = None
		self.consumer_tag = None
		self.running = False
		
		# RabbitMQ connection parameters
		self.host = "rabbitmq"  # Update with your RabbitMQ host, on linux use the container name
		self.port = 5672
		self.username = "admin"
		self.password = "password"
		self.virtual_host = "/"
        
	def connect(self):
		"""Establish connection to RabbitMQ"""
		system.util.getLogger("RabbitMQ").info("RabbitMQ start connection")
		try:
			factory = ConnectionFactory()
			factory.setHost(self.host)
			factory.setPort(self.port)
			factory.setUsername(self.username)
			factory.setPassword(self.password)
			factory.setVirtualHost(self.virtual_host)
			
			# Connection recovery settings
			factory.setAutomaticRecoveryEnabled(True)
			factory.setNetworkRecoveryInterval(5000)  # 5 seconds
						
			self.connection = factory.newConnection()
			self.channel = self.connection.createChannel()
			
			# Declare queue (make sure it exists)
			self.channel.queueDeclare(self.queue_name, True, False, False, None)
			
			system.util.getLogger("RabbitMQ").info("Connected to RabbitMQ successfully")
			return True
			
		except Exception as e:
			system.util.getLogger("RabbitMQ").error("Failed to connect to RabbitMQ: " + str(e))
			return False
    
	def create_consumer(self):
		"""Create the message consumer"""
		class MessageConsumer(DefaultConsumer):
			def __init__(self, channel, parent_consumer):
				DefaultConsumer.__init__(self, channel)
				self.parent = parent_consumer
                
			def handleDelivery(self, consumerTag, envelope, properties, body):
				try:
                    # Convert message body to string
					message = String(body, "UTF-8")
                    
                    # Process the message
					self.parent.process_message(message, envelope, properties)
                    
                    # Acknowledge the message
					self.getChannel().basicAck(envelope.getDeliveryTag(), False)
                    
				except Exception as e:
					system.util.getLogger("RabbitMQ").error("Error processing message: " + str(e))
					# Reject the message and requeue it
					self.getChannel().basicNack(envelope.getDeliveryTag(), False, True)
        
		return MessageConsumer(self.channel, self)
    
	def process_message(self, message, envelope, properties):
		"""Process received message and update Ignition tags/events"""
		try:
			logger = system.util.getLogger("RabbitMQ")
			logger.info("Received message: " + str(message))
			
			# get message count
			current_count = system.tag.readBlocking([self.tag_path_prefix + "MessageCount"])[0].value
			if current_count is None:
				current_count = 0
			
			tagPaths = []
			tagValues = []
			# Update a tag with the latest message
			tagPaths.append(self.tag_path_prefix + "LastMessage")
			tagValues.append(str(message))
			#Update message count
			tagPaths.append(self.tag_path_prefix + "MessageCount")
			tagValues.append(current_count + 1)
			# Update timestamp
			tagPaths.append(self.tag_path_prefix + "LastMessageTime")
			tagValues.append(system.date.now())
			
			#write to tags
			system.tag.writeBlocking(tagPaths,tagValues)
            
            # Parse JSON messages if applicable
			try:
				import json
				message_data = system.util.jsonDecode(str(message))
				
				# Update individual fields as tags
				for key, value in message_data.items():
					tag_path = self.tag_path_prefix + "Data/" + str(key)
					system.tag.writeBlocking([tag_path], [value])
					system.util.getLogger("RabbitMQ").info(tag_path)
                    
			except:
				# Not JSON, treat as plain text
				pass
            
            # Trigger custom events if needed
            # You can fire system events here that other scripts can listen to
            
		except Exception as e:
			system.util.getLogger("RabbitMQ").error("Error in process_message: " + str(e))
    
	def run(self):
		"""Main consumer loop"""
		self.running = True
		logger = system.util.getLogger("RabbitMQ")
		while self.running:
			try:
				if not self.connection or not self.connection.isOpen():
					if not self.connect():
						logger.warn("Failed to connect, retrying in 10 seconds...")
						time.sleep(10)
						continue
						
                # Set up consumer
				consumer = self.create_consumer()
				system.util.getLogger("RabbitMQ").info("RabbitMQ consumer created")
				
				# Start consuming
				self.consumer_tag = self.channel.basicConsume(self.queue_name, False, consumer)
				logger.info("Started consuming from queue: " + self.queue_name)
				
				# Keep the consumer alive
				while self.running and self.connection.isOpen():
					time.sleep(1)
                    
			except Exception as e:
				logger.error("Error in consumer loop: " + str(e))
				self.disconnect()
				time.sleep(10)  # Wait before reconnecting
    
	def disconnect(self):
		"""Clean disconnect from RabbitMQ"""
		try:
			if self.consumer_tag and self.channel:
				self.channel.basicCancel(self.consumer_tag)
			if self.channel and self.channel.isOpen():
				self.channel.close()
			if self.connection and self.connection.isOpen():
				self.connection.close()
		except Exception as e:
			system.util.getLogger("RabbitMQ").error("Error during disconnect: " + str(e))
    
	def stop(self):
		"""Stop the consumer"""
		self.running = False
		self.disconnect()

# Global variable to hold consumer reference
rabbitmq_consumer = None

# Start the consumer
def start_consumer():
	"""Start the RabbitMQ consumer"""
	global rabbitmq_consumer
	
	# Use a dedicated cache in system globals
	CACHE_ROOT_KEY = "rabbitmqCache"
	cache = system.util.getGlobals().setdefault(CACHE_ROOT_KEY, {})
	
	# Check if already running
	if cache.get("consumer_instance") is not None:
		return  # Already running
    
	try:
		# Create and start consumer
		rabbitmq_consumer = RabbitMQConsumer("ignition-actions")  # Replace with your queue name
		
		# Start in a separate thread
		executor = Executors.newSingleThreadExecutor()
		executor.submit(rabbitmq_consumer)
		
		# Store reference in dedicated cache
		cache["consumer_instance"] = rabbitmq_consumer
		cache["executor"] = executor
		
		system.util.getLogger("RabbitMQ").info("RabbitMQ consumer started successfully")
        
	except Exception as e:
		system.util.getLogger("RabbitMQ").error("Failed to start RabbitMQ consumer: " + str(e))



# Gateway Event Script - Application Shutdown Event
# Stop the consumer
def stop_consumer():
    """Stop the RabbitMQ consumer gracefully"""
    
    try:
        # Get cache from system globals
        CACHE_ROOT_KEY = "rabbitmqCache"
        cache = system.util.getGlobals().setdefault(CACHE_ROOT_KEY, {})
        
        rabbitmq_consumer = cache.get("consumer_instance")
        executor = cache.get("executor")
        
        if rabbitmq_consumer is not None:
            rabbitmq_consumer.stop()
            system.util.getLogger("RabbitMQ").info("RabbitMQ consumer stopped")
            
        if executor is not None:
            executor.shutdown()
            
        # Clean up cache
        cache.clear()
        
    except Exception as e:
        system.util.getLogger("RabbitMQ").error("Error stopping RabbitMQ consumer: " + str(e))

