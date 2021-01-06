# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for full license information.
import paho.mqtt.client as mqtt
import time
import random
import json

import argparse
from argparse import RawTextHelpFormatter

import logging
logging.basicConfig(level=logging.INFO, 
    format='%(asctime)s.%(msecs)03d - %(levelname)s - [%(filename)s:%(funcName)s] %(message)s', 
    datefmt='%d-%b-%y %H:%M:%S')
logger = logging.getLogger(__name__) 

def parse_cmdline_args():

    example_text =  'Example:\n'
    example_text += 'To create 10 clients with names "device0", "device1", ..., "device9" sending a random message every 0.5 seconds:\n' 
    example_text += '\n'
    example_text += '   python sim_clients.py -c 10 -n device -i 0.5\n'
    example_text += '\n'

    parser = argparse.ArgumentParser(
        description="A program to create multiple MQTT clients sending random data.",
        epilog=example_text, formatter_class=RawTextHelpFormatter)

    parser._action_groups.pop()

    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')
    
    required.add_argument(
        '-c', 
        default=20,
        dest='clients_num',
        required=True,
        help='Number of mqtt clients to be created.\nDefault is 20.'
    )

    required.add_argument(
        '-n', 
        default='client',
        dest='clients_root_name',
        required=True,
        help='client ID is generated by appending an incremental number "i" to this root name.'
    )

    required.add_argument(
        '-i', 
        default=0.5,
        dest='interval',
        required=True,
        help='interval (in seconds) between messages from a specific devices.\nExample: 0.5'
    )

    optional.add_argument(
        '--broker-ip', 
        default="127.0.0.1",
        dest='broker_ip',
        help='ip address of the MQTT broker.\nDefault is 127.0.0.1'
    )

    optional.add_argument(
        '--broker-port', 
        default=1883,
        dest='broker_port',
        help='port of the MQTT broker.\nDefault is 1883'
    )
    
    return parser.parse_args()

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    #logging.info("Connected with result code {}".formaty(str(rc)))
    if rc==0:
        logger.info("connected OK. Returned code={}".format(rc))
    else:
        logger.error("Bad connection. Returned code={}".format(rc))

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    logger.info("message received: {} - {}".format(msg.topic,msg.payload))

# args from the command line
parser = parse_cmdline_args()
clients_num = int(parser.clients_num)
interval = float(parser.interval)
root_name = parser.clients_root_name
broker_ip = parser.broker_ip
broker_port = int(parser.broker_port)

#list of clients
clients=[]

logger.info("Starting...")
logger.info("Press CTRL+C to STOP...")
time.sleep(2)

#creates clients
for i in range(clients_num):
    client_name = root_name + str(i)
    logger.info("creating client with client_id = {}".format(client_name))
    
    client = mqtt.Client(client_name)
    client.client_name = client_name
    clients.append(client)

#connects clients to broker
for client in clients:
    client.tls_set('/home/dipadmin/ca.crt')
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(broker_ip, broker_port, 60)
    client.loop_start()

#main loop to send data
try:
    while True:

        #loops the clients
        for client in clients:
            
            #builds the message to be sent
            topic = "device/{}/message".format(client.client_name)
            payload = {
                "param1": random.randrange(0,100),
                "param2": random.random()
            }

            #sends data only if client is connected to broker
            if client.is_connected():
                client.publish(topic, json.dumps(payload))
                logger.info("{} - message sent: {} - {}".format(client.client_name, topic,json.dumps(payload)))
            else:
                logger.error("{} is not connected to the broker!".format(client.client_name))
        
        #waits 
        time.sleep(interval)

except KeyboardInterrupt:
    logger.info("Terminated by the user.")