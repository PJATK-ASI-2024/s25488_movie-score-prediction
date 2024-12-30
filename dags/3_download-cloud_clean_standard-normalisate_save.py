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
from sklearn.preprocessing import StandardScaler, MinMaxScaler, LabelEncoder
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import matplotlib.pyplot as plt

#ID arkusza Google oraz ścieżka do pliku z poświadczeniami (znajduja sie one lokalnie)
SPREADSHEET_ID = '1DrW6OOAFmXqnS9I1fSAjKUz-ki1U63EJSVyZ5Ny0Uh0'
CREDENTIALS_PATH = '/opt/airflow/dags/creds.json'
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

VISUALIZATION_DIR = "/opt/airflow/visualizations/"
os.makedirs(VISUALIZATION_DIR, exist_ok=True)

#Pobiera dane z arkusza Google Sheets ("Zbiór modelowy").
def get_data_from_google_sheets():
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_PATH, SCOPE)
    client = gspread.authorize(creds)

    sheet = client.open_by_key(SPREADSHEET_ID).worksheet("Zbiór modelowy")
    data = sheet.get_all_records()
    
    df = pd.DataFrame(data)
    print("shape:",df.shape)
    df.to_csv("raw_model_data.csv", index=False)
    print("Data fetched from Google Sheets and saved locally as 'raw_model_data.csv'.")

#Czyści dane, usuwając brakujące wartości i duplikaty
def clean_data():
    file_path = "raw_model_data.csv"
    df = pd.read_csv(file_path)
    print("shape:",df.shape)
   
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
    print("Initial shape:", df.shape)

    # Splitting features and target
    X = df.drop(columns=['imdb_score'], axis=1)
    y = df['imdb_score']

    # Identify categorical and numerical columns
    categorical_columns = X.select_dtypes(include=['object']).columns
    numerical_columns = X.select_dtypes(exclude=['object']).columns

    print(f"Categorical columns (ignored for scaling): {list(categorical_columns)}")
    print(f"Numerical columns: {list(numerical_columns)}")

    # Standardize numerical columns only
    scaler = StandardScaler()
    X_scaled = X.copy()
    X_scaled[numerical_columns] = scaler.fit_transform(X[numerical_columns])
    print(f"Shape after standardizing numerical data: {X_scaled.shape}")

    # Normalize numerical columns only
    normalizer = MinMaxScaler()
    X_scaled[numerical_columns] = normalizer.fit_transform(X_scaled[numerical_columns])

    # Add target column back
    X_scaled['imdb_score'] = y

    # Save processed data
    docker_path = "/opt/airflow/processed_data"
    os.makedirs(docker_path, exist_ok=True)

    X_scaled.to_csv("/opt/airflow/processed_data/processed_model_data.csv", index=False)
    print("Processed data saved as '/opt/airflow/processed_model_data.csv'.")


#Wysyła przetworzone dane do arkusza Google Sheets ("Cleaned Data").
def upload_cleaned_data_to_google_sheets():
    file_path = "/opt/airflow/processed_data/processed_model_data.csv"
    df = pd.read_csv(file_path)

    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_PATH, SCOPE)
    print("creds found!")

    client = gspread.authorize(creds)

    sheet_name = "Cleaned Data"

    sheet = client.open_by_key(SPREADSHEET_ID).worksheet(sheet_name)
    sheet.clear()
    print("sheet cleared")
    print("shape:",df.shape)
    data = [df.columns.tolist()] + df.astype(str).values.tolist()
    print("data prepared, updating sheet")
    sheet.update(values=data, range_name="A1")

    print(f"Cleaned data successfully uploaded to Google Sheets in sheet '{sheet_name}'.")

# Visualization function using matplotlib
def create_visualizations():
    file_path = "/opt/airflow/processed_data/processed_model_data.csv"
    df = pd.read_csv(file_path)

    # Create distribution plots for numerical columns
    numerical_columns = df.select_dtypes(include=["number"]).columns
    for col in numerical_columns:
        plt.figure(figsize=(10, 6))
        plt.hist(
            df[col], bins=30, color="skyblue", edgecolor="black", alpha=0.7
        )
        plt.title(f"Distribution of {col}", fontsize=16)
        plt.xlabel(col, fontsize=14)
        plt.ylabel("Frequency", fontsize=14)
        plt.grid(axis="y", linestyle="--", alpha=0.7)
        plot_path = f"{VISUALIZATION_DIR}{col}_distribution.png"
        plt.savefig(plot_path)
        plt.close()
        print(f"Saved distribution plot for {col} at {plot_path}")

    # Create correlation heatmap
    plt.figure(figsize=(12, 8))
    correlation = df[numerical_columns].corr()
    plt.imshow(correlation, cmap="coolwarm", interpolation="nearest")
    plt.colorbar()
    plt.xticks(range(len(numerical_columns)), numerical_columns, fontsize=10, rotation=45, ha="right")
    plt.yticks(range(len(numerical_columns)), numerical_columns, fontsize=10)
    plt.title("Correlation Heatmap", fontsize=16)
    heatmap_path = f"{VISUALIZATION_DIR}correlation_heatmap.png"
    plt.savefig(heatmap_path)
    plt.close()
    print(f"Saved correlation heatmap at {heatmap_path}")

# Add visualization task to the DAG
with DAG(
    dag_id="data_processing_dag",
    description="Fetch, clean, process, and upload model data",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    fetch_task = PythonOperator(
        task_id="get_data_from_google_sheets",
        python_callable=get_data_from_google_sheets,
        retries=3,
        retry_delay=timedelta(minutes=1),
    )

    clean_task = PythonOperator(
        task_id="clean_data",
        python_callable=clean_data,
        retries=3,
        retry_delay=timedelta(minutes=1),
    )

    process_task = PythonOperator(
        task_id="standardize_and_normalize",
        python_callable=standardize_and_normalize,
        retries=3,
        retry_delay=timedelta(minutes=1),
    )

    visualization_task = PythonOperator(
        task_id="create_visualizations",
        python_callable=create_visualizations,
        retries=3,
        retry_delay=timedelta(minutes=1),
    )

    upload_task = PythonOperator(
        task_id="upload_cleaned_data_to_google_sheets",
        python_callable=upload_cleaned_data_to_google_sheets,
        retries=3,
        retry_delay=timedelta(minutes=1),
    )

    fetch_task >> clean_task >> process_task >> visualization_task >> upload_task
