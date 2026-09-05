import os
import re
import csv
import time
import datetime
import json
from airflow import DAG
from datetime import date, datetime, timedelta
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Specify the path to your JSON file
file_path = "/data/CCM_Data/Parameters.json"
# Open the JSON file and load its contents
with open(file_path, "r") as file:
    Parameters = json.load(file)

# To connect to LS using API standard product by Airflow Operator
Tenant_Name = Parameters['Tenant_Name']
Tenant_ID = Parameters['Tenant_ID']
kibo_environment_name = Parameters['kibo_environment_name']
kibo_webapi_url = Parameters['kibo_webapi_url'] + '/api'

# Please enter the email id of the user you want to be reported of all errors and failures
batch_error_email_id = 'abc.def@o9solutions.com'


params = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    "o9AF_tenant_id": Tenant_ID,
    "o9AF_tenant_name": Tenant_Name,
    "o9AF_environment_name": kibo_environment_name,
    "o9AF_webapi_url": kibo_webapi_url,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5)
}

with DAG(
        start_date=datetime(2022, 12, 9),
        dag_id="CCM_MasterDAG",
        params=params,
        schedule_interval =None,
        # schedule_interval="30 10,13,16,19,22 * * 1-5",
        # schedule_interval="30 8/3 * * 1-5",30th minute of the hour, starting from 8 AM UTC and repeating every 4 hours
        catchup=False,

) as dag:

    trigger_AirflowStats = TriggerDagRunOperator(
        task_id="trigger_AirflowStats",
        trigger_dag_id="CCM_AirflowStats",
        execution_date="{{ execution_date }}",
        email_on_failure=True,
        email=batch_error_email_id,
        trigger_rule="none_failed_min_one_success"
    )

    check_status_for_AirflowStats = ExternalTaskSensor(
           task_id='check_status_for_AirflowStats',
           external_dag_id='CCM_AirflowStats',
           external_task_id='end',
           allowed_states='',
           poke_interval=60,
           timeout=3600
    )

    # trigger_ConfigBackup = TriggerDagRunOperator(
    #     task_id="trigger_ConfigBackup",
    #     trigger_dag_id="CCM_ConfigBackup",
    #     execution_date="{{ execution_date }}",
    #     email_on_failure=True,
    #     email=batch_error_email_id,
    #     trigger_rule="none_failed_min_one_success"
    # )

    # check_status_for_CCM_ConfigBackup = ExternalTaskSensor(
    #     task_id='check_status_for_CCM_ConfigBackup',
    #     external_dag_id='CCM_ConfigBackup',
    #     external_task_id='end',
    #     allowed_states='',
    #     poke_interval=60,
    #     timeout=3600
    # )

    trigger_ModelStats = TriggerDagRunOperator(
        task_id="trigger_ModelStats",
        trigger_dag_id="CCM_ModelStats",
        execution_date="{{ execution_date }}",
        email_on_failure=True,
        email=batch_error_email_id,
        trigger_rule="all_success"
    )

    check_status_for_CCM_ModelStats = ExternalTaskSensor(
        task_id='check_status_for_CCM_ModelStats',
        external_dag_id='CCM_ModelStats',
        external_task_id='end',
        allowed_states='',
        poke_interval=60,
        timeout=3600
    )

    trigger_ProductStats = TriggerDagRunOperator(
        task_id="trigger_ProductStats",
        trigger_dag_id="CCM_ProductStats",
        execution_date="{{ execution_date }}",
        email_on_failure=True,
        email=batch_error_email_id,
        trigger_rule="none_failed_min_one_success"
    )

    check_status_for_CCM_ProductStats = ExternalTaskSensor(
        task_id='check_status_for_CCM_ProductStats',
        external_dag_id='CCM_ProductStats',
        external_task_id='end',
        allowed_states='',
        poke_interval=60,
        timeout=3600
    )

    trigger_SolverLogs = TriggerDagRunOperator(
        task_id="trigger_SolverLogs",
        trigger_dag_id="CCM_SolverLogs",
        execution_date="{{ execution_date }}",
        email_on_failure=True,
        email=batch_error_email_id,
        trigger_rule="none_failed_min_one_success"
    )

    check_status_for_CCM_SolverLogs = ExternalTaskSensor(
        task_id='check_status_for_CCM_SolverLogs',
        external_dag_id='CCM_SolverLogs',
        external_task_id='end',
        allowed_states='',
        poke_interval=60,
        timeout=3600
    )

    trigger_UserAdoption = TriggerDagRunOperator(
        task_id="trigger_UserAdoption",
        trigger_dag_id="CCM_UserAdoption",
        execution_date="{{ execution_date }}",
        email_on_failure=True,
        email=batch_error_email_id,
        trigger_rule="none_failed_min_one_success"
    )

    check_status_for_CCM_UserAdoption = ExternalTaskSensor(
        task_id='check_status_for_CCM_UserAdoption',
        external_dag_id='CCM_UserAdoption',
        external_task_id='end',
        allowed_states='',
        poke_interval=60,
        timeout=3600
    )

    trigger_AirflowStats >> check_status_for_AirflowStats >> trigger_ModelStats

    # trigger_ConfigBackup >> check_status_for_CCM_ConfigBackup >> trigger_ModelStats
    trigger_ModelStats >> check_status_for_CCM_ModelStats >> trigger_ProductStats >> check_status_for_CCM_ProductStats >>\
    trigger_SolverLogs >>check_status_for_CCM_SolverLogs >> trigger_UserAdoption >> check_status_for_CCM_UserAdoption