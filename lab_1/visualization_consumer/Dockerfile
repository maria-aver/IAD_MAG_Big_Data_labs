FROM python:3.13

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY visualization_consumer/ ./visualization_consumer

ENV PYTHONUNBUFFERED=1

CMD ["python", "visualization_consumer/app.py"]