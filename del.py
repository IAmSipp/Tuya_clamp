import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient

# 1. โหลดค่าคอนฟิกจากไฟล์ .env
load_dotenv()

INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET")

# 2. เชื่อมต่อฐานข้อมูล
client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
delete_api = client.delete_api()

# 3. กำหนดช่วงเวลาที่ต้องการลบ (ลบตั้งแต่อดีตจนถึงวินาทีปัจจุบัน)
start_time = "1970-01-01T00:00:00Z"
stop_time = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

print(f"🗑️ กำลังลบข้อมูลทั้งหมดใน Bucket: {INFLUX_BUCKET}...")

try:
    # 4. ส่งคำสั่งลบ โดยระบุเงื่อนไขให้ลบเฉพาะ measurement "ct_clamp_power"
    delete_api.delete(
        start=start_time,
        stop=stop_time,
        predicate='_measurement="ct_clamp_power"',
        bucket=INFLUX_BUCKET,
        org=INFLUX_ORG
    )
    print("✅ ลบข้อมูลสำเร็จ! ฐานข้อมูลของคุณสะอาดพร้อมรับข้อมูลจริงแล้ว")
except Exception as e:
    print(f"❌ เกิดข้อผิดพลาด: {e}")

client.close()