import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    dag_id="exercise1bonus",
    default_args={
        "owner": "godatadriven",
        "start_date": airflow.utils.dates.days_ago(3),
    },
)


def print_exec_date(execution_date, **context):
    print(execution_date)


print_exec_date = PythonOperator(
    task_id="print_exec_date",
    python_callable=print_exec_date,
    provide_context=True,
    dag=dag,
)
