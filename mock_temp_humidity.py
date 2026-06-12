import os
import random
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

load_dotenv()

INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")

TARGET_BUCKET = os.getenv("INFLUX_BUCKET_SENSOR")

influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

end_time = datetime.now(timezone.utc)
start_time = end_time - timedelta(days=3)
current_time = start_time

dev_id = "mock_env_001"
points_batch = []

print(f"กำลังสร้างข้อมูล Mock ตั้งแต่ {start_time.strftime('%Y-%m-%d %H:%M')} ถึง {end_time.strftime('%Y-%m-%d %H:%M')}")

while current_time <= end_time:
    hour = current_time.hour
    if 8 <= hour <= 18:
        # ช่วงกลางวัน: อุณหภูมิสูง ความชื้นต่ำ (เช่น 28 - 34°C / 40 - 60%)
        base_temp = random.uniform(28.0, 34.0)
        base_humid = random.uniform(40.0, 60.0)
    else:
        # ช่วงกลางคืน: อุณหภูมิต่ำ ความชื้นสูง (เช่น 24 - 27°C / 65 - 85%)
        base_temp = random.uniform(24.0, 27.0)
        base_humid = random.uniform(65.0, 85.0)

    # ใส่ Noise
    mock_temp = base_temp + random.uniform(-1.0, 1.0)
    mock_humid = base_humid + random.uniform(-2.0, 2.0)

    point = (
        Point("sensors_indoor")
        .tag("device_id", dev_id)
        .field("temperature", float(mock_temp))
        .field("humidity", float(mock_humid))
        .field("collect_time_string", current_time.strftime('%Y-%m-%d %H:%M:%S'))
        .time(current_time)
    )

    points_batch.append(point)

    current_time += timedelta(minutes=5)

    if len(points_batch) >= 500:
        write_api.write(bucket=TARGET_BUCKET, org=INFLUX_ORG, record=points_batch)
        points_batch = []
if points_batch:
    write_api.write(bucket=TARGET_BUCKET, org=INFLUX_ORG, record=points_batch)

write_api.close()
influx_client.close()

print(f"✅ บันทึกข้อมูล Mock อุณหภูมิและความชื้น ย้อนหลัง 3 วันลง Bucket '{TARGET_BUCKET}' สำเร็จแล้ว")