FROM apache/airflow:2.10.3 as airflow-base
ADD requirements.txt /requirements.txt
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r /requirements.txt

FROM python:3.10-slim as python-base

WORKDIR /app

COPY app.py ./
COPY models .//models/
COPY creds.json ./
COPY predict.py ./
COPY datasets ./datasets/
COPY requirements.txt ./
COPY model_summary.txt ./

RUN pip install --no-cache-dir -r requirements.txt

FROM python-base

COPY --from=airflow-base /root/.local /root/.local

EXPOSE 5000

# Uruchamianie FastAPI wewnątrz kontenera
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "5000"]