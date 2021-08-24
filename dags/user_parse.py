from datetime import datetime, timedelta
import json

from airflow import DAG
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import PythonOperator

from utils import _local_to_s3, run_redshift_external_query

# Config
BUCKET_NAME = Variable.get("BUCKET")
EMR_ID = Variable.get("EMR_ID")
EMR_STEPS = {}
with open("./dags/scripts/emr/clean_parse_review.json") as json_file:
    EMR_STEPS = json.load(json_file)

# DAG definition
default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "wait_for_downstream": True,
    "start_date": datetime(2021, 5, 23),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "user_behaviour",
    default_args=default_args,
    schedule_interval="0 0 * * *",
    max_active_runs=1,
)

# Use SQL to extract information from local csv
extract_user_parse_data = PostgresOperator(
    dag=dag,
    task_id="extract_user_parse_data",
    sql="./scripts/sql/unload_user_parse.sql",
    postgres_conn_id="postgres_default",
    params={"user_parse": "/temp/user_parse.csv"},
    depends_on_past=True,
    wait_for_downstream=True,
)

# Have the SQL from previous step moved to S3
user_parse_to_stage_data_lake = PythonOperator(
    dag=dag,
    task_id="user_parse_to_stage_data_lake",
    python_callable=_local_to_s3,
    op_kwargs={
        "file_name": "/temp/user_parse.csv",
        "key": "stage/user_parse/{{ ds }}/user_parse.csv",
        "bucket_name": BUCKET_NAME,
        "remove_local": "true",
    },
)

user_parse_stage_data_lake_to_stage_tbl = PythonOperator(
    dag=dag,
    task_id="user_parse_stage_data_lake_to_stage_tbl",
    python_callable=run_redshift_external_query,
    op_kwargs={
        "qry": "alter table spectrum.user_parse_staging add if not exists partition(insert_date='{{ ds }}') \
            location 's3://"
        + BUCKET_NAME
        + "/stage/user_parse/{{ ds }}'",
    },
)

party_review_to_raw_data_lake = PythonOperator(
    dag=dag,
    task_id="party_review_to_raw_data_lake",
    python_callable=_local_to_s3,
    op_kwargs={
        "file_name": "/data/party_review.csv",
        "key": "raw/party_review/{{ ds }}/party.csv",
        "bucket_name": BUCKET_NAME,
    },
)

spark_script_to_s3 = PythonOperator(
    dag=dag,
    task_id="spark_script_to_s3",
    python_callable=_local_to_s3,
    op_kwargs={
        "file_name": "./dags/scripts/spark/uniquep_text_classification.py",
        "key": "scripts/uniquep_text_classification.py",
        "bucket_name": BUCKET_NAME,
    },
)

start_emr_party_classification_script = EmrAddStepsOperator(
    dag=dag,
    task_id="start_emr_party_classification_script",
    job_flow_id=EMR_ID,
    aws_conn_id="aws_default",
    steps=EMR_STEPS,
    params={
        "BUCKET_NAME": BUCKET_NAME,
        "raw_party_review": "raw/party_review",
        "text_classifier_script": "scripts/uniquep_text_classifier.py",
        "stage_party_review": "stage/party_review",
    },
    depends_on_past=True,
)

last_step = len(EMR_STEPS) - 1

wait_for_party_classification_transformation = EmrStepSensor(
    dag=dag,
    task_id="wait_for_party_classification_transformation",
    job_flow_id=EMR_ID,
    step_id='{{ task_instance.xcom_pull("start_emr_party_classification_script", key="return_value")['
    + str(last_step)
    + "] }}",
    depends_on_past=True,
)

generate_user_parse_metric = PostgresOperator(
    dag=dag,
    task_id="generate_user_parse_metric",
    sql="scripts/sql/generate_user_parse_metric.sql",
    postgres_conn_id="redshift",
)

#Can edit to become different task. Filler for now.
end_of_data_pipeline = DummyOperator(task_id="end_of_data_pipeline", dag=dag)

extract_user_parse_data >> user_parse_to_stage_data_lake >> user_parse_stage_data_lake_to_stage_tbl
[
    party_review_to_raw_data_lake,
    spark_script_to_s3,
] >> start_emr_party_classification_script >> wait_for_party_classification_transformation
[
    user_parse_stage_data_lake_to_stage_tbl,
    wait_for_party_classification_transformation,
] >> generate_user_parse_metric >> end_of_data_pipeline
