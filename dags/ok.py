from pendulum import datetime
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator
from great_expectations.core.batch import BatchRequest
from great_expectations.data_context.types.base import DataContextConfig, CheckpointConfig

from airflow import DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1, tz="America/Sao_Paulo"),
}

with DAG(
    "great",
    schedule=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
):
    ge_data_context_root_dir_with_checkpoint_name_pass = GreatExpectationsOperator(
    task_id="ge_data_context_root_dir_with_checkpoint_name_pass",
    data_context_root_dir='/opt/airflow/great_expectations/',
    checkpoint_name="my_checkpoint",
)
