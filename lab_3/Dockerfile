FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY data ./data
COPY lakehouse ./lakehouse

ENV PYTHONPATH=/app/src

CMD ["python", "src/05_train_ml.py"]