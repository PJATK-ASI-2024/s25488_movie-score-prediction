from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime
import pandas as pd
import pickle
from sklearn.metrics import accuracy_score, precision_score, recall_score
import os

DATA_PATH = '/opt/airflow/processed_data/processed_model_data.csv'
MODEL_DIR = '/opt/airflow/models/'
CRITICAL_THRESHOLD = 0.60
EMAIL_RECIPIENT = 's25488@pjwstk.edu.pl'

def load_model_and_data():
    model_path = f"{MODEL_DIR}xgboost_accuracy_0.7957.pkl"
    if not os.path.exists(model_path):
        raise FileNotFoundError("Trained model not found.")
    
    with open(model_path, 'rb') as f:
        model = pickle.load(f)
    
    df = pd.read_csv(DATA_PATH)
    if df.empty:
        raise ValueError("New data is empty.")
    df['imdb_binned_score'] = pd.cut(
        df['imdb_score'], bins=[0, 4, 6, 8, 10], labels=[0, 1, 2, 3], right=True
    ).astype(int)
    X = df.drop(['imdb_score', 'imdb_binned_score'], axis=1).select_dtypes(include=['number'])
    y = df['imdb_binned_score']

    return model, X, y

def evaluate_model(ti):
    model, X, y = load_model_and_data()
    y_pred = model.predict(X)
    
    accuracy = accuracy_score(y, y_pred)
    precision = precision_score(y, y_pred, average='weighted')
    recall = recall_score(y, y_pred, average='weighted')
    
    metrics = {
        'accuracy': accuracy,
        'precision': precision,
        'recall': recall
    }
    
    ti.xcom_push(key='metrics', value=metrics)
    
    if accuracy < CRITICAL_THRESHOLD:
        raise ValueError(f"Model accuracy below threshold: {accuracy:.2f} (Threshold: {CRITICAL_THRESHOLD:.2f})")


def run_tests(ti):
    """Run unit tests on model and pipeline."""
    model, X, y = load_model_and_data()
    failed_tests = []

    # Test: Model should correctly predict results
    try:
        model.predict(X)
    except Exception as e:
        failed_tests.append(f"Model failed to predict: {str(e)}")
    
    # Test: Metric calculation
    try:
        y_pred = model.predict(X)
        accuracy_score(y, y_pred)
    except Exception as e:
        failed_tests.append(f"Metric calculation failed: {str(e)}")
    
    # Test: Handle missing data
    try:
        X_missing = X.copy()
        X_missing.iloc[0] = None
        model.predict(X_missing.fillna(0))
    except Exception as e:
        failed_tests.append(f"Pipeline failed with missing data: {str(e)}")
    
    # Push failed tests to XCom
    ti.xcom_push(key='failed_tests', value=failed_tests)
    
    if failed_tests:
        raise ValueError("Some tests failed.")


def send_alert(ti, **kwargs):
    """Send email alert in case of failure."""
    metrics = ti.xcom_pull(task_ids='evaluate_model', key='metrics')
    failed_tests = ti.xcom_pull(task_ids='run_tests', key='failed_tests')
    
    accuracy = metrics.get('accuracy', 'N/A')
    precision = metrics.get('precision', 'N/A')
    recall = metrics.get('recall', 'N/A')
    
    failed_tests_report = "\n".join(failed_tests) if failed_tests else "All tests passed."
    
    email_subject = "Model Validation Alert: Issues Detected"
    email_body = f"""
    <h3>Model Validation Alert</h3>
    <p><b>Model Performance Metrics:</b></p>
    <ul>
        <li>Accuracy: {accuracy:.4f}</li>
        <li>Precision: {precision:.4f}</li>
        <li>Recall: {recall:.4f}</li>
    </ul>
    <p><b>Failed Tests:</b></p>
    <pre>{failed_tests_report}</pre>
    """
    
    email_operator = EmailOperator(
        task_id='send_email',
        to=EMAIL_RECIPIENT,
        subject=email_subject,
        html_content=email_body
    )
    email_operator.execute(context=kwargs)

with DAG(
    dag_id='model_validation_monitoring',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    task_evaluate_model = PythonOperator(
        task_id='evaluate_model',
        python_callable=evaluate_model,
        provide_context=True
    )
    
    task_run_tests = PythonOperator(
        task_id='run_tests',
        python_callable=run_tests,
        provide_context=True
    )
    
    task_send_alert = PythonOperator(
        task_id='send_alert',
        python_callable=send_alert,
        trigger_rule='one_failed',
        provide_context=True
    )
    
    [task_evaluate_model, task_run_tests] >> task_send_alert
