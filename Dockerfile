FROM apache/airflow:2.10.3 as airflow-base
ADD requirements.txt /requirements.txt
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r /requirements.txt

FROM python:3.10-slim as python-base

WORKDIR /app

COPY app.py /app/
COPY models /app/models/
COPY creds.json /app/
COPY predict.py /app/
COPY datasets /app/datasets/
COPY requirements.txt /app/

RUN pip install --no-cache-dir -r requirements.txt

FROM python-base

COPY --from=airflow-base /root/.local /root/.local

EXPOSE 5000

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "5000"]
