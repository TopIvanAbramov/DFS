FROM python:3.7.9-alpine3.11
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY client.py .
CMD ["python", "client.py"]