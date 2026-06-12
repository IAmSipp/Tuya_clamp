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
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET_CLAMP")

influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

end_time = datetime.now(timezone.utc)
start_time = end_time - timedelta(days=3)
current_time = start_time

dev_id = "mock_device_001"
points_batch = []

print(f"กำลังสร้างข้อมูล Mock ตั้งแต่ {start_time.strftime('%Y-%m-%d %H:%M')} ถึง {end_time.strftime('%Y-%m-%d %H:%M')}")

while current_time <= end_time:
    hour = current_time.hour
    if 8 <= hour <= 18:
        # ช่วงกลางวัน คนอยู่บ้าน/ออฟฟิศ แอร์ทำงาน ใช้ไฟเยอะ (สมมติกระแส 5 - 15 แอมป์)
        base_current = random.uniform(5.0, 15.0)
    elif 19 <= hour <= 23:
        # ช่วงหัวค่ำ ปิดแอร์บางส่วน เปิดทีวี (2 - 8 แอมป์)
        base_current = random.uniform(2.0, 8.0)
    else:
        # กลางคืนดึกๆ หลับหมดแล้ว เปิดแค่ตู้เย็น (0.5 - 2 แอมป์)
        base_current = random.uniform(0.5, 2.0)

    # ใส่ Noise
    mock_current = base_current + random.uniform(-0.5, 0.5)
    if mock_current < 0.1: 
        mock_current = 0.1
        
    # จำลองค่า Power คร่าวๆ (P = V * I * PF) สมมติแรงดัน 220V และ Power Factor 0.9
    mock_power = mock_current * 220 * 0.9

    point_current = (
        Point("ct_clamp_power")
        .tag("device_id", dev_id)
        .tag("phase", "cur_current")
        .field("value", float(mock_current))
        .time(current_time)
    )
    
    point_power = (
        Point("ct_clamp_power")
        .tag("device_id", dev_id)
        .tag("phase", "cur_power")
        .field("value", float(mock_power))
        .time(current_time)
    )

    points_batch.extend([point_current, point_power])

    current_time += timedelta(minutes=5)

    if len(points_batch) >= 500:
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points_batch)
        points_batch = []

if points_batch:
    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points_batch)

write_api.close()
influx_client.close()

print("✅ บันทึกข้อมูล Mock ย้อนหลัง 3 วันลง InfluxDB สำเร็จแล้ว")