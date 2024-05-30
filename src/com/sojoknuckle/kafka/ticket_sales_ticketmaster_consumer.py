from kafka import KafkaConsumer
import json

# Setup the Kafka consumer connection to the broker.
consumer = KafkaConsumer(
 bootstrap_servers='<Service URI Host>:<Port>',
 security_protocol="SSL",
 ssl_cafile="ca.pem",
 ssl_certfile="service.cert",
 ssl_keyfile="service.key",
 auto_offset_reset='earliest'
)

print ("Printing messages recieved from the ticket_sales_ticketmaster Kafka topic")

# subscribe to the ticketmaster kafka topic to process the ticket master concert tickets.
consumer.subscribe(topics='ticket_sales_ticketmaster')
for message in consumer:
  print ("Partition:%d: Offset: %d" % (message.partition,
                          		message.offset))

  # Print the pretty-printed JSON string
  json_object = json.loads(message.value)
  json_formatted_str = json.dumps(json_object, indent=2)
  print(json_formatted_str)
