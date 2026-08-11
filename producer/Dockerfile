FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY simulate_devices.py ./

CMD ["python", "simulate_devices.py"]
