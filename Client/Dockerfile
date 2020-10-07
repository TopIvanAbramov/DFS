FROM python:3.7.9-alpine3.11
COPY requirements.txt .
COPY .env .
RUN pip install -r requirements.txt
COPY client.py .
WORKDIR /client
CMD ["python", "/client.py"]