import random
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def extraction_ventes():
    print("Extraction des ventes depuis la source...")

def nettoyage_donnees():
    if random.random() < 0.6:
        raise Exception("Erreur simulee: donnees source illisibles")
    print("Nettoyage des donnees...")

def agregation():
    print("Agrégation des ventes par région...")

def chargement_dashboard():
    print("Chargement des résultats vers le dashboard...")

def notifier_echec(context):
    with open("/opt/airflow/logs/notifications.log", "a") as f:
        f.write(
            f"ECHEC dag={context['dag'].dag_id} "
            f"task={context['task_instance'].task_id} "
            f"date={context['logical_date']}\n"
        )

with DAG(
    dag_id="pipeline_ventes",
    description="Pipeline guidé du TP Jour 1",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["tp-jour1"],
) as dag:
    
    t_extraction = PythonOperator(
        task_id="extraction_ventes",
        python_callable=extraction_ventes,
    )
    
    t_nettoyage = PythonOperator(
        task_id="nettoyage_donnees",
        python_callable=nettoyage_donnees,
        retries=3,
        retry_delay=timedelta(seconds=30),
        on_failure_callback=notifier_echec,
    )
    
    t_agregation = PythonOperator(
        task_id="agregation",
        python_callable=agregation,
    )
    
    t_chargement = PythonOperator(
        task_id="chargement_dashboard",
        python_callable=chargement_dashboard,
    )
    
    t_extraction >> t_nettoyage >> t_agregation >> t_chargement