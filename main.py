import paho.mqtt.client as mqtt
import threading
import time
import json


# ============================ defines =========================================

BROKER_ADDRESS     = "argus.paris.inria.fr"
RECORD_FILE        = "poipoi.log"

BASE_TOPIC         = "/charlie"
BASE_TOPIC_REPLAY  = "/charlie_replay"

# ============================ classes =========================================

class Recorder:

    #======================== public ==========================================
    def __init__(self):

        # store params

        # local variables
        self.mqttconnected             = False
        self.start_time                = time.time()
        self.message_index             = 0

        # connect to MQTT
        self.mqttclient                = mqtt.Client("Recorder")
        self.mqttclient.on_connect     = self._on_mqtt_connect
        self.mqttclient.on_message     = self._on_mqtt_message
        self.mqttclient.connect(BROKER_ADDRESS)

    def start(self):
        self._get_last_message_index()
        self.mqttclient.loop_forever()

    # ======================== private =========================================

    def _on_mqtt_connect(self, client, userdata, flags, rc):

        # remember I'm now connected
        self.mqttconnected   = True

        # subscribe desired channels
        self.mqttclient.subscribe(BASE_TOPIC + "/#")

        print("Recorder connected")


    def _on_mqtt_message(self, client, userdata, message):
        file_entry = {}
        file_entry["timestamp"] = time.time()
        file_entry["topic"] = message.topic
        file_entry["payload"] = json.loads(message.payload)
        file_entry["message_index"] = self.message_index

        with open(RECORD_FILE, "a") as file:
            # TODO: do I need to add the timestamp manually or does it come with it already?

            file.write(json.dumps(file_entry) + "\n")
            self.message_index += 1
            print("Got new message: {}".format(json.dumps(file_entry)))


    def _get_last_message_index(self):
        try:
            with open(RECORD_FILE, 'r') as f:
                lines = f.read().splitlines()
                if len(lines) == 0:
                    return
                last_line = lines[-1]
                file_entry = json.loads(last_line)
                self.message_index = file_entry["message_index"] + 1
        except KeyError:
            print("Last message in log file has no index")
        except FileNotFoundError:
            pass

class Replayer:
    MINIMUM_SLEEP_DURATION = 0
    SLEEP_UNTIL_MULTIPLIER = 0.8

    # ======================== public ==========================================
    def __init__(self):

        # store params

        # local variables
        self.mqttconnected             = False
        self.start_time                = time.time()

        # connect to MQTT
        self.mqttclient                = mqtt.Client("Replayer")
        self.mqttclient.on_connect     = self._on_mqtt_connect
        self.mqttclient.connect(BROKER_ADDRESS)
        self.mqttclient.loop_start()


    def start(self, speed_multiplier=1):
        begin_replay_ts = time.time()
        first_message_ts = 0
        record_replay_delta = 0
        first_current_message_delta = 0

        time.sleep(1)

        if not self.mqttconnected:
            raise Exception("MQTT not connected")

        with open(RECORD_FILE, "r") as file:
            for line in file:
                file_entry = json.loads(line)

                if first_message_ts == 0:
                    first_message_ts = file_entry["timestamp"]

                first_current_message_delta = file_entry["timestamp"] - first_message_ts

                self._sleep_until(begin_replay_ts + first_current_message_delta/speed_multiplier)

                topic = file_entry["topic"].replace(BASE_TOPIC, BASE_TOPIC_REPLAY)
                self.mqttclient.publish(
                    topic=topic,
                    payload=json.dumps(file_entry["payload"]),
                )
                print("Message sent!")

        self.mqttclient.loop_stop()

    # ======================== private =========================================
    def _on_mqtt_connect(self, client, userdata, flags, rc):

        # remember I'm now connected
        self.mqttconnected   = True


    def _sleep_until(self, wake_up_ts):
        sleep_duration = wake_up_ts - time.time()

        while sleep_duration > self.MINIMUM_SLEEP_DURATION:
            time.sleep(sleep_duration)
            sleep_duration = wake_up_ts - time.time()


if __name__ == '__main__':
    a = 1

    if a == 0:
        recorder = Recorder()
        recorder.start()
    elif a == 1:
        replayer = Replayer()
        replayer.start(speed_multiplier=3)
