import datetime
import os

from airflow import configuration
from airflow import models
from airflow.contrib.operators import dataflow_operator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.utils.dates import days_ago

bucket_path = models.Variable.get("bucket_path")
project_id = models.Variable.get("project_id")
gce_zone = models.Variable.get("gce_zone")
gce_region = models.Variable.get("gce_region")
DATAFLOW_FILE = os.path.join(
    configuration.get('core', 'dags_folder'), 'dataflow', 'df-02.py')
global PROJECT
global BUCKET
global DATASET
PROJECT = 'hig-bigqueryproject'
BUCKET = 'test_buk'
DATASET = 'hevo_test_1'
DEFAULT_DAG_ARGS = {
    "start_date": days_ago(1),
    "dataflow_default_options": {
         'project': PROJECT,
        # 'bucket': BUCKET,
        # 'dataset': DATASET,
         "temp_location": bucket_path + "/temp/",
         "runner" : "DataflowRunner",
        # "labels" : "airflow-version=v1-10-10-composer",
        # "job_name" : "my-task-2983818b",
        # "region" : "us-central1"
    },
}
#'bucket': 'test_buk',
#'dataset': 'hevo_test_1',
with models.DAG(
    dag_id="composer_dataflowpythonoperator_dag_v_2",
    description="A Dag Script",
    schedule_interval=None,default_args=DEFAULT_DAG_ARGS,) as dag:
    job_args = {
        # "project": project_id,
        # "region": gce_region,
        # "zone": gce_zone,
        'project': PROJECT,
        'bucket': BUCKET,
        'dataset': DATASET,
    }
    dataflow_task = dataflow_operator.DataFlowPythonOperator(
        task_id='my_task',
        py_file=DATAFLOW_FILE,
        options=job_args
    )