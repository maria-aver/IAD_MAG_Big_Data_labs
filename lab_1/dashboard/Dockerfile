FROM python:3.13

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY dashboard/ ./dashboard

ENV PYTHONUNBUFFERED=1

CMD ["streamlit", "run", "dashboard/app.py", "--server.address=0.0.0.0", "--server.port=8501"]