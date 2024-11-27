from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
import kagglehub
import os
import shutil
import gspread
from oauth2client.service_account import ServiceAccountCredentials

SPREADSHEET_ID = '1DrW6OOAFmXqnS9I1fSAjKUz-ki1U63EJSVyZ5Ny0Uh0'
CREDENTIALS_PATH = '/opt/airflow/dags/creds.json'
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

def download_dataset():
    download_dir = "datasets"
    file_path = f"{download_dir}/movie_metadata.csv"
    
    os.makedirs(download_dir, exist_ok=True)
    
    path = kagglehub.dataset_download("carolzhangdc/imdb-5000-movie-dataset", force_download=True)
    source_path = f"{path}/movie_metadata.csv"

    shutil.copy(source_path, file_path)
    print(f"Dataset downloaded to {file_path}")
    return file_path

def split_dataset():
    file_path = "datasets/movie_metadata.csv"
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Dataset file not found at {file_path}")

    data = pd.read_csv(file_path)
    train_data, further_training_data = train_test_split(data, test_size=0.3, random_state=42)

    train_path = "train.csv"
    future_path = "future.csv"
    train_data.to_csv(train_path, index=False)
    further_training_data.to_csv(future_path, index=False)

    print(f"Train dataset saved to: {train_path}")
    print(f"Future training dataset saved to: {future_path}")

def upload_to_google_sheets():
    train_path = "train.csv"
    future_path = "future.csv"
    
    if not os.path.exists(train_path) or not os.path.exists(future_path):
        raise FileNotFoundError("Train or future dataset files not found. Ensure split_dataset ran successfully.")

    train_data = pd.read_csv(train_path)
    future_training_data = pd.read_csv(future_path)

    save_to_google_sheets(train_data, SPREADSHEET_ID, "Zbiór modelowy", CREDENTIALS_PATH, SCOPE)
    save_to_google_sheets(future_training_data, SPREADSHEET_ID, "Zbiór douczeniowy", CREDENTIALS_PATH, SCOPE)

def save_to_google_sheets(df, spreadsheet_id, sheet_name, credentials_path, scope):
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
    client = gspread.authorize(creds)
    
    try:
        sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        print(f"Sheet named '{sheet_name}' not found in the spreadsheet.")
        return

    sheet.clear()
    print(f"Sheet '{sheet_name}' cleared before inserting new data.")
    
    df = df.replace([np.inf, -np.inf, np.nan], "")
    print("shape:",df.shape)
    data = [df.columns.tolist()] + df.astype(str).values.tolist()
    sheet.update("A1", data)

    print(f"Data successfully updated in Google Sheets: {sheet_name}")

with DAG(
    dag_id='download_split_dag',
    description='Download, split data, and save to Google Sheets',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    download_task = PythonOperator(
        task_id='download_dataset',
        python_callable=download_dataset,
        retries=3,
        retry_delay=timedelta(minutes=1)
    )

    split_task = PythonOperator(
        task_id='split_dataset',
        python_callable=split_dataset,
        retries=3,
        retry_delay=timedelta(minutes=1)
    )

    upload_task = PythonOperator(
        task_id='upload_to_google_sheets',
        python_callable=upload_to_google_sheets,
        retries=3,
        retry_delay=timedelta(minutes=1)
    )

    download_task >> split_task >> upload_task
