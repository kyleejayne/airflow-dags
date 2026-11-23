"""
nhl_gold_dag.py   (DAG 3 of 3)

No schedule. Runs only when the nhl_silver DAG triggers it.
Create a VM, run gold.py on it (rebuild the last 3 days of the Gold tables,
refresh player_form, and export one CSV per table), then delete the VM.
Nothing to trigger after this; Tableau reads the CSVs in gold/tableau/.

    create_resource_group >> create_network >> create_vm >> run_gold >> delete_vm
"""

import os
import sys
from datetime import datetime, timedelta

from airflow.sdk import DAG, Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.smtp.notifications.smtp import SmtpNotifier

# The DAGs are synced into a subfolder of the dags folder, so add this file's own
# folder to the import path before importing the shared helper next to it
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import nhl_utils as vm

default_args = {
    "retries": 2,                                   # try each task up to 3 times in total
    "retry_delay": timedelta(minutes=5),
    # Email when a task has used up all its retries (uses the smtp_default connection)
    "on_failure_callback": SmtpNotifier(to=Variable.get("ALERT_EMAIL")),
}

with DAG(
    dag_id="nhl_gold",
    description="NHL skater game stats: Silver -> Gold + Tableau CSVs, triggered by nhl_silver",
    schedule=None,                                  # triggered by nhl_silver only
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["nhl", "medallion", "gold"],
):
    rg = PythonOperator(task_id="create_resource_group", python_callable=vm.create_resource_group)
    network = PythonOperator(task_id="create_network", python_callable=vm.create_network)
    create = PythonOperator(task_id="create_vm", python_callable=vm.create_vm)
    run = PythonOperator(task_id="run_gold", python_callable=vm.run_layer, op_args=["gold"])
    delete = PythonOperator(task_id="delete_vm", python_callable=vm.delete_vm,
                            trigger_rule="all_done")                # always clean up, even on failure

    rg >> network >> create >> run >> delete