import os
import time
from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
from airflow.hooks.base import BaseHook
from databricks.sdk import WorkspaceClient

conn = BaseHook.get_connection("databricks_default")

host = conn.host

if not host.startswith("http"):
    host = f"https://{host}"


@dag
def orchestrate():

    @task
    def run_databricks_job(jobid):        
        ws = WorkspaceClient(
            host = host,
            token = conn.password,

        )


        job_trigger = ws.jobs.run_now(
            job_id = jobid 
        )


        while True:
            job_run = ws.jobs.get_run(job_trigger.run_id)
            if job_run.state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:
                if job_run.state.result_state == RunResultState.SUCCESS:
                    print("Job completed successfully!")
                    break
                else:
                    raise Exception(f"Job failed with state: {job_run.state.result_state}")

            time.sleep(5)
        return "run databricks job Completed"


















   
    @task.bash
    def clean_target():
        return "rm -rf /opt/airflow/omnichannel_retail/target && rm -rf /opt/airflow/omnichannel_retail/logs"

    @task.bash
    def source_freshness():
        # Install dbt dependencies first to ensure packages are available in the container,
        # then run the source freshness command.
        return "cd /opt/airflow/omnichannel_retail && dbt source freshness"
    


# silver run
    silver_postgresql = BashOperator(
        task_id="silver_postgresql",
        cwd= "/opt/airflow/omnichannel_retail",
        bash_command="dbt run --select path:models/silver/postgresql"
    )


    silver_api = BashOperator(
        task_id="silver_api",
        cwd= "/opt/airflow/omnichannel_retail",
        bash_command="dbt run --select path:models/silver/api"
    )

    silver_s3 = BashOperator(
        task_id="silver_s3",
        cwd= "/opt/airflow/omnichannel_retail",
        bash_command="dbt run --select path:models/silver/s3"
    )



# silver test

    silver_postgresql_test = BashOperator(
        task_id="silver_postgresql_test",
        cwd= "/opt/airflow/omnichannel_retail",
        bash_command="dbt test --select path:models/silver/postgresql"
    )


    silver_api_test = BashOperator(
        task_id="silver_api_test",
        cwd= "/opt/airflow/omnichannel_retail",
        bash_command="dbt test --select path:models/silver/api"
    )

    silver_s3_test = BashOperator(
        task_id="silver_s3_test",
        cwd= "/opt/airflow/omnichannel_retail",
        bash_command="dbt test --select path:models/silver/s3"
    )




# run seed
    upload_seed = BashOperator(
        task_id="upload_seed",
        cwd= "/opt/airflow/omnichannel_retail",
        bash_command="dbt seed"
    )


# run snapshot

    snapshot = BashOperator(
        task_id="snapshot",
        cwd= "/opt/airflow/omnichannel_retail",
        bash_command="dbt snapshot"
    )


# gold run

    gold_calender = BashOperator(
        task_id="gold_calender",
        cwd= "/opt/airflow/omnichannel_retail",
        bash_command="dbt run --select path:models/gold/dim_calender"
    )


    gold_dim = BashOperator(
        task_id="gold_dim",
        cwd= "/opt/airflow/omnichannel_retail",
        bash_command="dbt run --select path:models/gold/dim"
    )

    gold_fact = BashOperator(
        task_id="gold_fact",
        cwd= "/opt/airflow/omnichannel_retail",
        bash_command="dbt run --select path:models/gold/fact"
    )



# gold test

    gold_calender_test = BashOperator(
        task_id="gold_calender_test",
        cwd= "/opt/airflow/omnichannel_retail",
        bash_command="dbt test --select path:models/gold/dim_calender"
    )


    gold_dim_test = BashOperator(
        task_id="gold_dim_test",
        cwd= "/opt/airflow/omnichannel_retail",
        bash_command="dbt test --select path:models/gold/dim"
    )



    gold_fact_test = BashOperator(
        task_id="gold_fact_test",
        cwd= "/opt/airflow/omnichannel_retail",
        bash_command="dbt test --select path:models/gold/fact"
    )





    postgresql_ingestion = run_databricks_job.override(task_id="postgresql_ingestion")(108918968410964)
    api_ingestion = run_databricks_job.override(task_id="api_ingestion")(871007555097493)
    s3_ingestion = run_databricks_job.override(task_id="s3_ingestion")(305147155039754)


    clean = clean_target()
    freshness = source_freshness()


    [
        postgresql_ingestion,
        api_ingestion,
        s3_ingestion,
    ] >> clean >> freshness # Clean → Freshness

  

    # Freshness → all Silver runs in parallel
    freshness >> [
        silver_postgresql,
        silver_api,
        silver_s3,
    ]

    # Each Silver run → its own tests
    silver_postgresql >> silver_postgresql_test
    silver_api >> silver_api_test
    silver_s3 >> silver_s3_test



    #metadata = generate_meradata()


    metadata = run_databricks_job.override(
        task_id="generate_metadata"
    )(811649033901095)


    [
        silver_postgresql_test,
        silver_api_test,
        silver_s3_test,
    ] >> metadata >> upload_seed >> snapshot 



    snapshot >> [gold_calender, gold_dim] 

    # Each Silver run → its own tests
    gold_calender >> gold_calender_test
    gold_dim >> gold_dim_test


    [gold_calender_test, gold_dim_test] >> gold_fact >> gold_fact_test



orchestrate_dag = orchestrate()