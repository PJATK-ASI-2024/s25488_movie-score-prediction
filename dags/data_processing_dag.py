"""
Co robi:
1. Pobiera dane z Google Sheets (arkusz "Zbiór modelowy").
2. Czyści dane, usuwając brakujące wartości i duplikaty.
3. Standaryzuje i normalizuje dane, przygotowując je do dalszego przetwarzania.
4. Wysyła przetworzone dane do arkusza Google Sheets ("Cleaned Data").

Kroki:
- `get_data_from_google_sheets`: Pobiera dane z Google Sheets i zapisuje je lokalnie.
- `clean_data`: Czyści dane (usuwa brakujące wartości i duplikaty).
- `standardize_and_normalize`: Standaryzuje i normalizuje dane.
- `upload_cleaned_data_to_google_sheets`: Wysyła przetworzone dane do Google Sheets.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, MinMaxScaler
import gspread
from oauth2client.service_account import ServiceAccountCredentials

#ID arkusza Google oraz ścieżka do pliku z poświadczeniami (znajduja sie one lokalnie)
SPREADSHEET_ID = '1DrW6OOAFmXqnS9I1fSAjKUz-ki1U63EJSVyZ5Ny0Uh0'
CREDENTIALS_PATH = '/opt/airflow/dags/creds.json'
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

#Pobiera dane z arkusza Google Sheets ("Zbiór modelowy").
def get_data_from_google_sheets():
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_PATH, SCOPE)
    client = gspread.authorize(creds)

    sheet = client.open_by_key(SPREADSHEET_ID).worksheet("Zbiór modelowy")
    data = sheet.get_all_records()
    
    df = pd.DataFrame(data)
    df.to_csv("raw_model_data.csv", index=False)
    print("Data fetched from Google Sheets and saved locally as 'raw_model_data.csv'.")

#Czyści dane, usuwając brakujące wartości i duplikaty
def clean_data():
    file_path = "raw_model_data.csv"
    df = pd.read_csv(file_path)
   
    df.drop(columns=['movie_imdb_link'], inplace=True, errors='ignore')
    df.dropna(inplace=True)
    print("Missing values handled: Rows with NaN dropped.")

    df.drop_duplicates(inplace=True)
    print("Duplicate rows removed.")

    df.to_csv("cleaned_model_data.csv", index=False)
    print("Cleaned data saved locally as 'cleaned_model_data.csv'.")

#Standaryzuje i normalizuje dane, przygotowując je do dalszego przetwarzania.
def standardize_and_normalize():
    file_path = "cleaned_model_data.csv"
    df = pd.read_csv(file_path)

    X = df.drop(columns=['imdb_score'], axis=1)
    y = df['imdb_score']
    X = pd.get_dummies(X, drop_first=True)

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    normalizer = MinMaxScaler()
    normalized_features = pd.DataFrame(normalizer.fit_transform(X_scaled), columns=X.columns)

    normalized_features['imdb_score'] = y 
    normalized_features.to_csv("processed_model_data.csv", index=False)
    print("Standardized and normalized data saved locally as 'processed_model_data.csv'.")

#Wysyła przetworzone dane do arkusza Google Sheets ("Cleaned Data").
def upload_cleaned_data_to_google_sheets():
    file_path = "processed_model_data.csv"
    df = pd.read_csv(file_path)

    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_PATH, SCOPE)
    print("creds found!")

    client = gspread.authorize(creds)

    sheet_name = "Cleaned Data"

    sheet = client.open_by_key(SPREADSHEET_ID).worksheet(sheet_name)
    sheet.clear()
    print("sheet cleared")

    data = [df.columns.tolist()] + df.astype(str).values.tolist()
    print("data prepared, updating sheet")
    sheet.update(values=data, range_name="A1")

    print(f"Cleaned data successfully uploaded to Google Sheets in sheet '{sheet_name}'.")

#Definicja DAG-a
with DAG(
    dag_id='process_model_data',
    description='Fetch, clean, process, and upload model data',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    fetch_task = PythonOperator(
        task_id='get_data_from_google_sheets',
        python_callable=get_data_from_google_sheets,
        retries=3,
        retry_delay=timedelta(minutes=1)
    )

    clean_task = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data,
        retries=3,
        retry_delay=timedelta(minutes=1)
    )

    process_task = PythonOperator(
        task_id='standardize_and_normalize',
        python_callable=standardize_and_normalize,
        retries=3,
        retry_delay=timedelta(minutes=1)
    )

    upload_task = PythonOperator(
        task_id='upload_cleaned_data_to_google_sheets',
        python_callable=upload_cleaned_data_to_google_sheets,
        retries=3,
        retry_delay=timedelta(minutes=1)
    )

    fetch_task >> clean_task >> process_task >> upload_task
