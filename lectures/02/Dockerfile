FROM python:latest

RUN pip install hdfs

ADD client.py .
ADD sensorsample.py .

CMD ["python", "./sensorsample.py"]
