import random
import time
import decimal
import uuid
import json
from client import get_hdfs_client

lastTime = time.time()
hdfs_client = get_hdfs_client()

def saveSample(sensorID, samplePayload):
	createdAt = time.time()

	payload: {
		"sensor_id": sensorID,
		"value": payload,
		"unit": "MW",
		"temporal_aspect": "real_time"
	}

	sample = {
		"payload": payload,
		"correlation_id": str(uuid.uuid4()),
		"created_at": createdAt,
		"schema_version": 1
	}

	sampleJSON = json.dumps(sample)

	timeDate = time.gmtime(createdAt)

	hdfs_client.write("/data/raw/sensor_id={sample.payload}/temporal_aspect={sample.payload.temporal_aspect}/year={timeDate.tm_year}/month={timeDate.tm_month}/day={timeDate.tm_wday}/{sample.correlation_id}.json", sampleJSON, encoding="utf-8", overwrite=False)

def fetchSample():
	sensorID = random.randInt(1, 7)
	samplePayload = float(decimal.Decimal(random.randrange(155, 389))/100)
	saveSample(sensorID, samplePayload)

if __name__ == "__main__": 
	while True:
		if(time.time() - lastTime >= 2):
			fetchSample()

