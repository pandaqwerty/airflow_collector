from datetime import datetime, timedelta

from airflow.operators.subdag_operator import SubDagOperator
from airflow.models import DAG, Variable
from tester_collector.subdags.sub import all_process

PROJECT_VERSION = '1.0'
PROJECT_NAME = 'tester-collector'

# MAIN DAGS
# interval = "0 3 */1 * *"
interval = "*/10 * * * *"
DAG_ID = 'tester_collector'
start_date = datetime.strptime(Variable.get("tester_collector_start_date"), "%Y-%m-%d %H:%M:%S")
emails = Variable.get('support_email_list').split(',')
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': start_date,
    'email': emails,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG(dag_id=DAG_ID, default_args=default_args, schedule_interval=interval, start_date=start_date) as dag:

    main_subdags_id = 'all_process'
    process_keywords_dag = SubDagOperator(
        task_id=main_subdags_id,
        subdag=all_process(
            "{0}.{1}".format(DAG_ID, main_subdags_id), start_date, interval, default_args, PROJECT_NAME, PROJECT_VERSION),
        depends_on_past=True,
        dag=dag
    )