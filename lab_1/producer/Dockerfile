FROM python:3.13

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY producer/ ./producer

ENV PYTHONUNBUFFERED=1

CMD ["python", "producer/app.py"]