import os
import json
import pulsar
from dotenv import load_dotenv
from mq_authentication import get_authentication
from message_util import decrypt_message, message_id
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

load_dotenv()

MQ_ENV_PROD = "event"
MQ_ENV_TEST = "event-test"
PULSAR_SERVER_SG = "pulsar+ssl://mqe-sg.iotbing.com:7285/"

ACCESS_ID = os.getenv("TUYA_ACCESS_ID")
ACCESS_KEY = os.getenv("TUYA_ACCESS_KEY")
PULSAR_SERVER_URL = PULSAR_SERVER_SG
MQ_ENV = os.getenv("TUYA_MQ_ENV", MQ_ENV_TEST)

INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET_CLAMP")

influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

def handle_message(pulsar_message, decrypt_mssage, msg_id):
    try:
        payload = json.loads(decrypt_mssage)
        print(payload)
        dev_id = payload.get("devId", "unknown_device")
        status_list = payload.get("status", [])
        
        for status in status_list:
            code = status.get("code")   
            value = status.get("value") 
            
            if code in ["cur_power", "cur_current"]:
                try:
                    float_value = float(value)
                    point = (
                        Point("ct_clamp_power")
                        .tag("device_id", dev_id)
                        .tag("phase", code)
                        .field("value", float_value)
                    )
                    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
                except (ValueError, TypeError):
                    pass

    except Exception:
        pass


client = pulsar.Client(PULSAR_SERVER_URL, 
    authentication=get_authentication(ACCESS_ID, ACCESS_KEY),
    tls_allow_insecure_connection=True,
    )

consumer = client.subscribe(ACCESS_ID + '/out/' + MQ_ENV, ACCESS_ID + '-sub', consumer_type=pulsar.ConsumerType.Failover)

while True:
    try:
        pulsar_message = consumer.receive()
        msg_id = message_id(pulsar_message.message_id())
        decrypt_mssage = decrypt_message(pulsar_message, ACCESS_KEY)
        
        handle_message(pulsar_message, decrypt_mssage, msg_id)
        consumer.acknowledge_cumulative(pulsar_message)
        
    except pulsar.Interrupted:
        break
    except Exception:
        pass

write_api.close()
influx_client.close()
client.close()
