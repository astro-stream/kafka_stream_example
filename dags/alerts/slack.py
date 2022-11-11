from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
import os

def task_success_slack_alert(context):
    slack_msg = f"""
            :tada: Task Success.
            *Task*: {context.get('task_instance').task_id}
            *Dag*: {context.get('task_instance').dag_id}
            *Execution Time*: {context.get('execution_date')}
            *Log Url*: {context.get('task_instance').log_url}
            *Lineage Graph*: {os.environ.get("OPENLINEAGE_URL")}/lineage/graph/job/{context["conf"].get("kubernetes", "namespace")}/{context["dag"].dag_id}.{context["ti"].task_id}
            """
    success_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id='slack-task-success',
        message=slack_msg,
        username='airflow')
    return success_alert.execute(context=context)

def task_failure_slack_alert(context):
    slack_msg = f"""
            :red_circle: Task Failure.
            *Task*: {context.get('task_instance').task_id}
            *Dag*: {context.get('task_instance').dag_id}
            *Execution Time*: {context.get('execution_date')}
            *Log Url*: {context.get('task_instance').log_url}
            *Lineage Graph*: {os.environ.get("OPENLINEAGE_URL")}/lineage/graph/job/{context["conf"].get("kubernetes", "namespace")}/{context["dag"].dag_id}.{context["ti"].task_id}
            """
    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id='slack-task-failure',
        message=slack_msg,
        username='airflow-test')
    return failed_alert.execute(context=context)

def task_retry_slack_alert(context):
    slack_msg = f"""
            :thinking_face: Task is retrying.
            *Task*: {context.get('task_instance').task_id}
            *Dag*: {context.get('task_instance').dag_id}
            *Execution Time*: {context.get('execution_date')}
            *Log Url*: {context.get('task_instance').log_url}
            *Lineage Graph*: {os.environ.get("OPENLINEAGE_URL")}/lineage/graph/job/{context["conf"].get("kubernetes", "namespace")}/{context["dag"].dag_id}.{context["ti"].task_id}
            """
    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id='slack-task-retry',
        message=slack_msg,
        username='airflow-test')
    return failed_alert.execute(context=context)