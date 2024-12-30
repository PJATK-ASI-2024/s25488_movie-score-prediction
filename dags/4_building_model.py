from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score
import pickle
import os

DATA_PATH = '/opt/airflow/processed_data/processed_model_data.csv'
MODEL_DIR = '/opt/airflow/models/'
REPORTS_DIR = '/opt/airflow/reports/'
VISUALIZATION_DIR = '/opt/airflow/visualizations/'
SUMMARY_FILE = f"{REPORTS_DIR}model_summary.txt"

os.makedirs(VISUALIZATION_DIR, exist_ok=True)
os.makedirs(MODEL_DIR, exist_ok=True)
os.makedirs(REPORTS_DIR, exist_ok=True)

def load_data():
    df = pd.read_csv(DATA_PATH)
    df['imdb_binned_score'] = pd.cut(
        df['imdb_score'], bins=[0, 4, 6, 8, 10], labels=[0, 1, 2, 3], right=True
    ).astype(int)
    X = df.drop(['imdb_score', 'imdb_binned_score'], axis=1).select_dtypes(include=['number'])
    y = df['imdb_binned_score']
    return train_test_split(X, y, test_size=0.3, random_state=42)

def write_summary(model_name, accuracy, model_path):
    with open(SUMMARY_FILE, 'a') as f:
        f.write(f"{model_name},{accuracy:.4f},{model_path}\n")

def train_gradient_boosting():
    X_train, X_test, y_train, y_test = load_data()
    model = GradientBoostingClassifier(n_estimators=50, learning_rate=0.09, max_depth=5)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    model_filename = f"{MODEL_DIR}gradient_boosting_accuracy_{accuracy:.4f}.pkl"
    with open(model_filename, 'wb') as f:
        pickle.dump(model, f)
    write_summary("gradient_boosting", accuracy, model_filename)

def train_random_forest():
    X_train, X_test, y_train, y_test = load_data()
    model = RandomForestClassifier(n_estimators=200)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    model_filename = f"{MODEL_DIR}random_forest_accuracy_{accuracy:.4f}.pkl"
    with open(model_filename, 'wb') as f:
        pickle.dump(model, f)
    write_summary("random_forest", accuracy, model_filename)

def train_xgboost():
    X_train, X_test, y_train, y_test = load_data()
    model = XGBClassifier(use_label_encoder=False, eval_metric='logloss')
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    model_filename = f"{MODEL_DIR}xgboost_accuracy_{accuracy:.4f}.pkl"
    with open(model_filename, 'wb') as f:
        pickle.dump(model, f)
    write_summary("xgboost", accuracy, model_filename)

def compare_and_save_best_model():
    if not os.path.exists(SUMMARY_FILE):
        raise ValueError("Summary file not found. Train models before comparison.")
    with open(SUMMARY_FILE, 'r') as f:
        lines = f.readlines()
    models = [line.strip().split(',') for line in lines]
    models = [(name, float(acc), os.path.abspath(path)) for name, acc, path in models]
    best_model_name, best_accuracy, best_model_path = max(models, key=lambda x: x[1])
    best_model_filename = os.path.abspath(f"{MODEL_DIR}best_model_{best_model_name}_accuracy_{best_accuracy:.4f}.pkl")
    report_filename = os.path.abspath(f"{REPORTS_DIR}best_model_report.txt")
    with open(report_filename, 'w') as f:
        f.write(f"Best Model: {best_model_name}\n")
        f.write(f"Accuracy: {best_accuracy:.4f}\n")
    print(f"Best model saved as {best_model_filename}")

with DAG(
    dag_id='model_training_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    task_train_gradient_boosting = PythonOperator(
        task_id='train_gradient_boosting',
        python_callable=train_gradient_boosting,
        retries=3,
        retry_delay=timedelta(minutes=1)
    )

    task_train_random_forest = PythonOperator(
        task_id='train_random_forest',
        python_callable=train_random_forest,
        retries=3,
        retry_delay=timedelta(minutes=1)
    )

    task_train_xgboost = PythonOperator(
        task_id='train_xgboost',
        python_callable=train_xgboost,
        retries=3,
        retry_delay=timedelta(minutes=1)
    )

    task_compare_and_save_best_model = PythonOperator(
        task_id='compare_and_save_best_model',
        python_callable=compare_and_save_best_model,
        retries=3,
        retry_delay=timedelta(minutes=1)
    )

    [task_train_gradient_boosting, task_train_random_forest, task_train_xgboost] >> task_compare_and_save_best_model
