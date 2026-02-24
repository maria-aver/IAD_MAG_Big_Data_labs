FROM python:3.13

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY ml_consumer/ ./ml_consumer

ENV PYTHONUNBUFFERED=1

CMD ["python", "ml_consumer/app.py"]