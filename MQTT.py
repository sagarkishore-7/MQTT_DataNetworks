import random
import time

# Install with PIP: 'pip install paho-mqtt'
from paho.mqtt import client as mqtt_client

# Address of the broker
broker = 'inet-mqtt-broker.mpi-inf.mpg.de'
port = 1883

# Topic name for login
topic = "login"

# Generate client ID with publish prefix randomly
client_id = f'python-mqtt-{random.randint(0, 100)}'

# MQTT Client login credentials
username = 'saki00003@stud.uni-saarland.de'
password = '7023593'

# Variable to store broker reply/message sent to client
reply = []

# Dictionary to store mapping between cmd sent by broker and the corresponding reply
reply_dict = {'CMD1': 'Apple',
              'CMD2': 'Cat',
              'CMD3': 'Dog',
              'CMD4': 'Rat',
              'CMD5': 'Boy',
              'CMD6': 'Girl',
              'CMD7': 'Toy',
              }

# Send this at last and compare for connection termination
final = 'Well done my IoT!'

# Setup Connection to MQTT Broker
def connect_mqtt() -> mqtt_client:

    def on_connect(client, userdata, flags, rc):

        """"" The value of rc determines if the connection is successful or not:
                0: Connection successful
                1: Incorrect protocol version
                2: Invalid Client Identifier
                3: Server Unavailable
                4: Bad Username or Password
                5: Not Authorised
                6 - 255: Currently unused """

        if rc == 0:
            print("Connected to MQTT Broker: " + broker)
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client

# Function to publish messages to MQTT broker on specific topics
def publish(client, msg, topic):
    result = client.publish(topic, msg)
    # result: [0, 1]
    status = result[0]
    if status == 0:
        print(f" Topic:{topic} , Message:{msg}")
    else:
        print(f"Failed to send message to topic {topic}")

# Subscribe to MQTT broker's messages to specific topics defaulting to topic='7023593/UUID'
def subscribe(client, topic='7023593/UUID'):

    # Function to decode broker's reply and append it to REPLY variable as a string
    def on_message(client, userdata, msg):
        # Message is a binary data. So, decode and convert it to string format before append
        reply.append(str(msg.payload.decode("utf-8")))
        print(f"\n Received {reply[0]} from {msg.topic} topic")

    client.subscribe(topic)
    client.on_message = on_message

# Driver Program
def run():
    client = connect_mqtt()
    client.loop_start()
    subscribe(client)
    time.sleep(1)
    publish(client, password, topic)
    time.sleep(1)
    subscribe(client, reply[0])
    # Wait 3 seconds after publishing UNIQUE ID to topic UNIQUE ID
    time.sleep(3)
    while True:
        if (len(reply) > 1):
            for command in reply[1:]:
                publish(client, reply_dict[command], reply[0] + '/' + command)
        # Compare reply with final to exit from loop and disconnect
        if (reply[-1] == final):
            break

    client.loop_stop()
    print("\n Disconnecting from MQTT Broker")
    client.disconnect()

# Invoke driver program
if __name__ == '__main__':
    run()
