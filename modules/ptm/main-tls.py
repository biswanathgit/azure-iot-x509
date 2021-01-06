# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for
# full license information.

import time
import json
import ssl
import paho.mqtt.client as mqtt
import libs.iotedge as iotedge


from azure.iot.device import Message
import os
ptm_output_name = "ptm_output"

global module_client
global module_id
module_id = os.environ["IOTEDGE_MODULEID"]

def ptm_forward(device_id, json_obj):
    global ptm_output_name
    global module_client
    msg = Message(json.dumps(json_obj))
    msg.custom_properties["leafdeviceid"] = device_id
    msg.custom_properties["moduleid"] = module_id
    module_client.send_message_to_output(msg, ptm_output_name)

def ptm_logic(msg):
    device_id = msg.topic.split("/")[1]
    print("Device ID: {}".format(device_id))
    ptm_output_obj = {
        "topic": msg.topic,
        "payload": json.loads(msg.payload.decode('utf-8'))
    }
    ptm_forward(device_id, ptm_output_obj)

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    print("on_connect")
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("device/client0/message")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(msg.topic+" =====> "+str(msg.payload))
    ptm_logic(msg)

module_client = iotedge.init()
module_client.send_message_to_output("Hello Biswanath ============= From demo_client ", ptm_output_name)

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.tls_set('/app/ca.crt')
client.connect("diptest01", 8884, 60)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()