FROM python:3.13

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY data_processor/ ./data_processor
COPY preprocessing/ ./data_processor/preprocessing

ENV PYTHONUNBUFFERED=1

CMD ["python", "data_processor/app.py"]