FROM python:3.11

RUN pip install hdfs panda pyarrow fastavro

ADD client.py .
ADD sensorsample.py .

CMD ["python", "./sensorsample.py"]
