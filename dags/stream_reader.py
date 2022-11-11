from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.decorators import task
from airflow_provider_kafka.operators.consume_from_topic import ConsumeFromTopicOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from astronomer.providers.databricks.operators.databricks import DatabricksSubmitRunOperatorAsync
import json, logging
import uuid 
from alerts.slack import (
    task_success_slack_alert,
    task_failure_slack_alert,
    task_retry_slack_alert
)

owner = 'astronomer'
dag_tags = ['kafka', 'streaming']

kafka_username = Variable.get("kafka-username")
kafka_pass = Variable.get("kafka-pass")
kafka_cluster = Variable.get("kafka-cluster")
env_consumer_group = Variable.get("kafka-consumer")

environ = 'DEV'
consumer_logger = logging.getLogger("airflow")

databricks_cluster = Variable.get("databricks-cluster")
databricks_kafka_notebook = Variable.get("databricks-notebook")

json_databricks = {
  "existing_cluster_id": databricks_cluster,
  "notebook_task": {
    "notebook_path" : databricks_kafka_notebook,
    "base_parameters": {"environment":environ,
                        "load_date": """{{ ds_nodash }}""",
                        }
  }
}

def consumer_function(msg, prefix=None, **kwargs):

    azurehook = WasbHook(wasb_conn_id="wasb_docker")
    filename = f"{datetime.now().strftime('%Y')}/{datetime.now().strftime('%m')}/{datetime.now().strftime('%d')}/{datetime.now().strftime('%H')}/{datetime.now().strftime('%M')}/{uuid.uuid4()}.txt"
    print(msg.value().decode('utf-8'))
    if azurehook.check_for_prefix(container_name="kafka-stream", prefix='raw/'+filename):
        # delete blob for idempotent run
        azurehook.delete_file(container_name="kafka-stream", blob_name='raw/'+filename)
        #reload blob
        azurehook.load_string(string_data=msg.value().decode('utf-8'), container_name="kafka-stream/raw", blob_name=filename)
    else:
        #Load api call to blob
        azurehook.load_string(string_data=msg.value().decode('utf-8'), container_name="kafka-stream/raw", blob_name=filename)

    return

default_args = {
    'owner': 'kafka-provider',
    'depends_on_past': False,
    'retries':2,
    'retry_delay': timedelta(seconds=45),
    'start_date': datetime(2022, 10, 29),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['grayson.stream@astronomer.io']
}

with DAG('streaming_etl',
    default_args=default_args,
    schedule_interval='0 14 * * *',
    catchup=False,
    start_date=datetime(2022, 10, 20),
    description='DAG that reads streaming data into a azure blob store.',
    tags=dag_tags,
    owner_links={"kafka-provider": "https://github.com/astronomer/airflow-provider-kafka"},):

    @task
    def start(owner="kafka-provider"):
        return 'start'

    consume_messages = ConsumeFromTopicOperator(
        task_id="consume_topic_to_adls",
        topics=["intent"],
        apply_function="stream_reader.consumer_function",
        apply_function_kwargs={"prefix": "consumed:::"},
        consumer_config={
            "bootstrap.servers": f"{kafka_cluster}:9092",
            "sasl.username":kafka_username,
            "sasl.password":kafka_pass,
            "group.id": env_consumer_group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "security.protocol":"SASL_SSL",
            "sasl.mechanisms":"PLAIN"
        },
        commit_cadence="end_of_batch",
        max_messages=100,
        max_batch_size=10,
        on_failure_callback=task_failure_slack_alert,
        on_success_callback=task_success_slack_alert,
    )

    databricks_transform_async = DatabricksSubmitRunOperatorAsync(
            task_id='databricks_transform', 
            json=json_databricks,
            databricks_conn_id='databricks_docker',
            polling_period_seconds=10,
            on_failure_callback=task_failure_slack_alert,
            on_success_callback=task_success_slack_alert,
      )

    @task
    def end(owner="kafka-provider"):
        return 'end'
    
    start() >> consume_messages >> databricks_transform_async >> end()
    
