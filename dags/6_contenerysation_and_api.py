from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import docker

# Funkcja do budowania obrazu Dockera
def build_docker_image():
    client = docker.from_env()
    client.images.build(path='.', tag='movie-prediction-api')
    print("Obraz Dockera zbudowany pomyślnie.")

# Funkcja do publikacji obrazu na Docker Hub
def push_docker_image():
    client = docker.from_env()
    client.images.push(repository='opaciorkowski/movie-prediction-api', tag='latest')
    print("Obraz Dockera opublikowany na Docker Hub.")

# Funkcja do uruchomienia kontenera Dockera
def run_docker_container():
    client = docker.from_env()
    container = client.containers.run(
        'movie-prediction-api',
        detach=True,
        ports={5000: 5000},
        name='movie-prediction-api-container'
    )
    print(f"Kontener uruchomiony: {container.name}")

with DAG(
    dag_id='docker_image_build_and_run',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    build_image_task = PythonOperator(
        task_id='build_docker_image',
        python_callable=build_docker_image,
        dag=dag,
    )

    push_image_task = PythonOperator(
        task_id='push_docker_image',
        python_callable=push_docker_image,
        dag=dag,
    )

    run_container_task = PythonOperator(
        task_id='run_docker_container',
        python_callable=run_docker_container,
        dag=dag,
    )

    build_image_task >> push_image_task >> run_container_task
