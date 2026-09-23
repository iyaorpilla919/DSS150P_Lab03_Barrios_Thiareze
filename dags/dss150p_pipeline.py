from __future__ import annotations
import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

default_args = {
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=2),
    "execution_timeout": pendulum.duration(minutes=15),
}

def on_failure_callback(context):
    ti = context["task_instance"]
    print(f"[FAILURE] dag={ti.dag_id} task={ti.task_id} run_id={ti.run_id} "
          f"try={ti.try_number} exception={context.get('exception')}")

@dag(
    dag_id="dss150p_pipeline",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    params={"run_mode": "full", "year": 2026, "month": 1},
    tags=["dss150p"],
)
def dss150p_pipeline():
    extract = BashOperator(
        task_id="extract",
        bash_command="cd /opt/airflow && python -m src.cli extract --run-id {{ run_id }}",
        on_failure_callback=on_failure_callback,
    )
    transform = BashOperator(
        task_id="transform",
        bash_command="cd /opt/airflow && python -m src.cli run-all --run-id {{ run_id }} --skip-extract",
        on_failure_callback=on_failure_callback,
    )
    load = BashOperator(
        task_id="load",
        bash_command=(
            "cd /opt/airflow && "
            "{% if params.run_mode == 'partition' %}"
            "python -m src.cli load-partition --year {{ params.year }} --month {{ params.month }} --run-id {{ run_id }}"
            "{% else %}"
            "python -m src.cli load --run-id {{ run_id }}"
            "{% endif %}"
        ),
        on_failure_callback=on_failure_callback,
    )
    validate = BashOperator(
        task_id="validate",
        bash_command="cd /opt/airflow && python -m src.cli validate --run-id {{ run_id }}",
        on_failure_callback=on_failure_callback,
    )
    extract >> transform >> load >> validate

dss150p_pipeline()