"""
claim_adjudication_pipeline.py
Triggers Step Functions adjudication workflow for all PENDING claims in staging.
Runs hourly. Claims older than 4 hours without a decision trigger a P2 alert.
"""
from airflow import DAG
from airflow.providers.amazon.aws.operators.step_function import (
    StepFunctionStartExecutionOperator,
)
from airflow.sensors.sql import SqlSensor
from datetime import datetime, timedelta

default_args = {
    "owner":   "claims-adjudication",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["claims-adjudication-alerts@insurer.com"],
}

with DAG(
    dag_id="claim_adjudication_pipeline",
    default_args=default_args,
    schedule_interval="0 * * * *",   # hourly
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["claims", "adjudication", "step-functions"],
) as dag:

    wait_for_staged_claims = SqlSensor(
        task_id="wait_for_staged_claims",
        conn_id="redshift_claims_prod",
        sql="""
            SELECT COUNT(*) FROM claims_staging.stg_claim_raw
            WHERE adjudication_status = 'PENDING'
            AND _ingested_at > CURRENT_TIMESTAMP - INTERVAL '1 hour'
        """,
        mode="poke",
        poke_interval=60,
        timeout=3600,
    )

    trigger_adjudication = StepFunctionStartExecutionOperator(
        task_id="trigger_adjudication_workflow",
        state_machine_arn="arn:aws:states:us-east-1:123456789:stateMachine:claim-adjudication-workflow",
        aws_conn_id="aws_claims_prod",
        input='{"source": "airflow", "batch_mode": true}',
    )

    wait_for_staged_claims >> trigger_adjudication
