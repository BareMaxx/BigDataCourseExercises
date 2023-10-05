import random
import time
import decimal
import uuid
import json
from client import get_hdfs_client

lastTime = time.time()
hdfs_client = get_hdfs_client()

def saveSample(sensorID, samplePayload):
    payload: {
        "sensor_id": sensorID,
        "value": payload,
        "unit": "MW",
        "temporal_aspect": "real_time"
    }

    sample = {
        "payload": payload,
        "correlation_id": uuid.uuid4(),
        "created_at": time.time(),
        "schema_version": 1
    }

    sampleJSON = json.dumps(sample)

    # save

def fetchSample():
	sensorID = random.randInt(1, 7)
	samplePayload = float(decimal.Decimal(random.randrange(155, 389))/100)
	saveSample(sensorID, samplePayload)

if __name__ == "__main__": 
	while true:
        if(time.time() - lastTime >= 2): # create sample every 2 seconds
            fetchSample()

