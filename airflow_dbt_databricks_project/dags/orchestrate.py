from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
import time
import pendulum

@dag(
    dag_id="orchestrate_dag",
    schedule="0 10 * * *",
    catchup=False,
    start_date=pendulum.datetime(2026, 10, 3, tz="UTC")
)
def orchestrate():
    @task
    def ingest_cdc():
        ws = WorkspaceClient()

        job_trigger = ws.jobs.run_now(job_id=1109396595478553)

        while True:
            job_status = ws.jobs.get_run(run_id=job_trigger.run_id)

            print(f"Job status: {job_status.state.life_cycle_state}, Result state: {job_status.state.result_state}")
            
            if job_status.state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:
                if job_status.state.result_state == RunResultState.SUCCESS:
                    print("Job completed successfully.")
                    break
                else:
                    raise Exception(f"Job failed with state: {job_status.state.result_state}")
                
            time.sleep(10)  # Wait for 10 seconds before checking the status again
        return "CDC ingestion completed successfully."


    @task.bash
    def clean_target():
        return "rm -rf /opt/airflow/walmart_project/target && rm -rf /opt/airflow/walmart_project/logs"


    @task.bash
    def source_freshness():
        return "cd /opt/airflow/walmart_project && dbt source freshness"

    silver_technical = BashOperator(
        task_id="silver_technical",
        bash_command="cd /opt/airflow/walmart_project && dbt run --select silver_tech"
    )

    silver_technical_tests = BashOperator(
        task_id="silver_technical_tests",   
        bash_command="cd /opt/airflow/walmart_project && dbt test --select silver_tech"
    )

    silver_business = BashOperator(
        task_id="silver_business",
        bash_command="cd /opt/airflow/walmart_project && dbt run --select silver_bus"
    )

    silver_business_tests = BashOperator(
        task_id="silver_business_tests",    
    bash_command="cd /opt/airflow/walmart_project && dbt test --select silver_bus"
    )   

    gold_ephermeral = BashOperator(
        task_id="gold_ephermeral",
        bash_command="cd /opt/airflow/walmart_project && dbt run --select gold/ephemeral"
    )

    gold_dimensions = BashOperator(
        task_id="gold_dimensions",
        bash_command="cd /opt/airflow/walmart_project && dbt snapshot"
    )

    gold_facts = BashOperator(
        task_id="gold_facts",
        bash_command="cd /opt/airflow/walmart_project && dbt run --select gold/facts"
    )

    ingest_cdc() >> clean_target() >> source_freshness() >> silver_technical >> silver_technical_tests >> silver_business >> silver_business_tests >> gold_ephermeral >> gold_dimensions >> gold_facts

orchestrate_dag = orchestrate()