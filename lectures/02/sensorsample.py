import random
import time
import decimal
import uuid
import json
from client import get_hdfs_client

lastTime = time.time()
hdfs_client = get_hdfs_client()

haveBeenCreated = {
	"1": False,
	"2": False,
	"3": False,
	"4": False,
	"5": False,
	"6": False,
	"7": False,
}

def saveSample(sensorID, samplePayload):
	createdAt = time.time()

	payload = {
		"sensor_id": sensorID,
		"value": samplePayload,
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

	if (haveBeenCreated[str(sensorID)] == False): 
		hdfs_client.write(f'/data4/raw/sensor_id={sample["payload"]["sensor_id"]}/temporal_aspect={sample["payload"]["temporal_aspect"]}/year={timeDate.tm_year}/month={timeDate.tm_mon}/day={timeDate.tm_wday}/sensor.json', sampleJSON, encoding="utf-8", append=False)
		haveBeenCreated[str(sensorID)] = True
	else:
		hdfs_client.write(f'/data4/raw/sensor_id={sample["payload"]["sensor_id"]}/temporal_aspect={sample["payload"]["temporal_aspect"]}/year={timeDate.tm_year}/month={timeDate.tm_mon}/day={timeDate.tm_wday}/sensor.json', sampleJSON, encoding="utf-8", append=True)
		
def fetchSample():
	sensorID = random.randint(1, 7)
	samplePayload = float(decimal.Decimal(random.randrange(155, 389))/100)
	saveSample(sensorID, samplePayload)

if __name__ == "__main__": 
	while True:
		if(time.time() - lastTime >= 2):
			fetchSample()

