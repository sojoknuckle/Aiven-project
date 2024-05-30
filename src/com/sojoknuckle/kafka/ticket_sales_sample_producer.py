from kafka import KafkaProducer
import json
import uuid
import random
import datetime
import time

# Setup the Kafka connection to the broker
producer = KafkaProducer(
 bootstrap_servers='<Service URI Host>:<Port>',
 security_protocol="SSL",
 ssl_cafile="ca.pem",
 ssl_certfile="service.cert",
 ssl_keyfile="service.key",
 value_serializer=lambda v: json.dumps(v).encode('ascii')
)

# Sample Data
SELLERS = ['VividSeats', 'TicketMaster', 'StubHub', 'SeatGeek']
CONCERT_NAMES = ['Taylor Swift - ERAS Tour', 'P!NK - Summer Carnival 2024','Creed - Summer of 99 Tour','Green Day - The Saviors Tour']
CONCERT_DATES = ['2024-07-14', '2024-07-24', '2024-09-17','2024-10-31','2024-11-06','2024-12-15']
FIRST_NAMES = ['John', 'Erik','Mark','Janet','Kathern','Kim']
LAST_NAMES = ['TadBrant', 'Cromwell','Langley','Gonzales','Finnegan','Zimmerman']
ADDRESSES = ['101 Some Rd Sometown NY 16790','1500 My Rd Philadephia PA 19106', '151 N right Rd cleveland OH 44105','5678 walker rd Los Angles CA 90001'] 


# Loop 100 times and generate random concert tickets
i = 1
while True:

   # Create the message data
   data = {}
   data['id'] = str(uuid.uuid1())
   data['tx_timestamp'] = datetime.datetime.now().isoformat()
   data['seller'] = random.choice(SELLERS)
   data['concert_name'] = random.choice(CONCERT_NAMES)
   data['concert_date'] = random.choice(CONCERT_DATES)
   data['customer_name'] = random.choice(FIRST_NAMES) + " " +  random.choice(LAST_NAMES)
   data['customer_address'] = random.choice(ADDRESSES)

   # Send message to Kafka topic
   producer.send(
      'ticket_sales_raw',
      value = data
   )
   producer.flush()

   # Print message that was kust sent to Kafka
   json_data = json.dumps(data, indent=2)
   print(json_data);
	
   # Sleep for 2 seconds between messages
   time.sleep(2)
   i += 1

   if (i > 100):
      break

