#!/usr/bin/env python
# coding: utf-8
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

import datetime

args = {"owner": "godatadriven", "start_date": datetime.datetime(2019, 10, 17)}

dag = DAG(
    dag_id="exercise2",
    default_args=args,
    schedule_interval="@daily",
    description="Sample DAG showing some Airflow Operators.",
    catchup=True
)


def _print_exec_date(execution_date, **context):
    print(execution_date)


print_date = PythonOperator(
    task_id="print_execution_date",
    python_callable=_print_exec_date,
    provide_context=True,
    dag=dag,
)

end = DummyOperator(task_id="the_end", dag=dag)

wait_tasks = [
    BashOperator(task_id="wait_{w}".format(w=w), bash_command="sleep {w}".format(w=w), dag=dag)
    for w in [1, 5, 10]
]

print_date >> wait_tasks >> end
