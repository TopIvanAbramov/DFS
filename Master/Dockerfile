FROM python:3.7.9-alpine3.11
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY datanode.py .
COPY master.py .
WORKDIR /master
COPY dfs.conf .
CMD ["python", "/master.py"]
