import re
import asyncio
import os
import traceback
from influxdb_client import Point
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from sh import tail
from datetime import datetime

# CONFIG ME
BUCKET = str(os.getenv("BUCKET", default=None))
TOKEN = str(os.getenv("TOKEN", default=None))
ORG = str(os.getenv("ORG", default=None))
INFLUXURL = str(os.getenv("INFLUXURL", default="http://localhost:8086/"))
LOG_FILE = str(os.getenv("LOG_FILE",default="/app/access.log"))
INTERVAL = int(os.getenv("INTERVAL", default=60))
DEBUG = bool(os.getenv("DEBUG", default=False))

logging.basicConfig(filename='line_read.log', level=logging.INFO, format='%(asctime)s - %(message)s')

async def write_influx(points):
    try:
        async with InfluxDBClientAsync(url=INFLUXURL, token=TOKEN, org=ORG) as client:
            write_api = client.write_api()
            await write_api.write(BUCKET, record=points)
            
            print(f'[{current_time}] Sent data to InfluxDB')
    except Exception as e:
        print(f'[{current_time}] Exception: {traceback.print_tb(e.__traceback__)}')


async def build_data(data):
    try:
        points = []
        miss_bytes = 0
        hit_bytes = 0
        current_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
        timestamp = data['timestamp']
        del data['timestamp']
        for k,v in data.items():
            p = Point("downloaded").time(timestamp)
            p.tag("ipaddress", k)
            for kk,vv in data[k].items():
                p.tag("platform", kk)
                print(p.tag())
                p.tag("url",kk)
                print(p.tag())
                for kkk,vvv in data[k][kk].items():
                    if kkk == "miss_bytes": miss_bytes = vvv
                    if kkk == "hit_bytes": hit_bytes = vvv
                    p.field(str(kkk), float(vvv))
                p.field("total_bytes", (miss_bytes + hit_bytes))
                points.append(p)
        await write_influx(points)
    except Exception as e:
        print(f'[{current_time}] Exception: {traceback.print_tb(e.__traceback__)}')

async def main():
    _lastrun = datetime.now()
    data = {"timestamp": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}
    for line in tail("-f", LOG_FILE, _iter=True):
        current_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
        try:
            if DEBUG: print(f'[{current_time}] DEBUG: {line}')
            if DEBUG: print(f'[{current_time}] DEBUG: {data}')
            result = re.search(r'\[(\w*)\] (\S+).*\[(\d{2}/\w+/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})\] "(GET|POST) (.+?) HTTP/\d\.\d" (\d{3}) (\d*) ".*" "(.*)" "(.*)" "(.*)" "(.*)"', line)
            if not result:
                if DEBUG: print(f'[{current_time}] DEBUG: No Result Found... Skipping')
                continue
            platform, ipaddress, client, cachehit, statuscode, bytes, endpoint, url = result.group(1), result.group(2), result.group(8), result.group(9), int(result.group(6)), int(result.group(7)), result.group(10), result.group(5)
            if ipaddress not in data.keys():
                data[ipaddress] = {}
            dp = data[ipaddress]
            if platform not in dp.keys():
                dp[platform] = {}
            if cachehit == "HIT":
                if "hits" not in dp[platform].keys():
                    dp[platform]['hits'] = 0
                if "hit_bytes" not in dp[platform].keys():
                    dp[platform]['hit_bytes'] = 0
                dp[platform]['hits'] = dp[platform]['hits'] + 1
                dp[platform]['hit_bytes'] = dp[platform]['hit_bytes'] + bytes
            else:
                if "misses" not in dp[platform].keys():
                    dp[platform]['misses'] = 0
                if "miss_bytes" not in dp[platform].keys():
                    dp[platform]['miss_bytes'] = 0
                dp[platform]['misses'] = dp[platform]['misses'] + 1
                dp[platform]['miss_bytes'] = dp[platform]['miss_bytes'] + bytes
            if statuscode not in dp[platform].keys():
                dp[platform][statuscode] = 1
            else:
                dp[platform][statuscode] = dp[platform][statuscode] + 1
            data[ipaddress] = dp
            if (datetime.now() - _lastrun).total_seconds() > INTERVAL:
                await build_data(data)
                data = {"timestamp": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}
                _lastrun = datetime.now()
        def process_line(line):
            nonlocal _lastrun, data
            current_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
            try:
                # Log each time a new line is read
                logging.info(f'[{current_time}] Read line: {line}')
            
                if DEBUG: print(f'[{current_time}] DEBUG: {line}')
                if DEBUG: print(f'[{current_time}] DEBUG: {data}')
        except Exception as e:
            print(f'[{current_time}] Exception: {traceback.print_tb(e.__traceback__)}')
        

current_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
print(f'[{current_time}] Started!')
print(f'[{current_time}] InfluxURL: {INFLUXURL}')
print(f'[{current_time}] Log File: {LOG_FILE}')
print(f'[{current_time}] Interval: {INTERVAL}')
if DEBUG: print(f'[{current_time}] DEBUG ENABLED')
asyncio.run(main())
