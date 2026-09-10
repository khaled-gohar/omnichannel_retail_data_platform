import os
import time
from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState




@dag
def orchestrate():

#     @task
#     def ingest_cdc():        
#         ws = WorkspaceClient(
#             host = os.getenv("DATABRICKS_HOST") or os.getenv("databricks_host", "https://dbc-993be504-9c5d.cloud.databricks.com"),
#             token = os.getenv("DATABRICKS_TOKEN") or os.getenv("databricks_token"),
#         )


#         job_trigger = ws.jobs.run_now(
#             job_id = 685556535042293
#         )


#         while True:
#             job_run = ws.jobs.get_run(job_trigger.run_id)
#             if job_run.state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:
#                 if job_run.state.result_state == RunResultState.SUCCESS:
#                     print("Job completed successfully!")
#                     break
#                 else:
#                     raise Exception(f"Job failed with state: {job_run.state.result_state}")

#             time.sleep(5)
#         return "CDC Ingestion Completed"
    
    @task.bash
    def clean_target():
        return "rm -rf /opt/airflow/omnichannel_retail/target && rm -rf /opt/airflow/omnichannel_retail/logs"

    @task.bash
    def source_freshness():
        # Install dbt dependencies first to ensure packages are available in the container,
        # then run the source freshness command.
        return "cd /opt/airflow/omnichannel_retail && dbt source freshness"
    
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

#     silver_technical_test = BashOperator(
#         task_id="silver_technical_test",
#         cwd= "/opt/airflow/walmart_project",
#         bash_command="dbt test --select path:models/silver_t"
#     )
    
    
#     silver_business = BashOperator(
#         task_id="silver_business",
#         cwd= "/opt/airflow/walmart_project",
#         bash_command="dbt run --select path:models/silver_b"
#     )   
    
#     silver_business_test = BashOperator(
#         task_id="silver_business_test",
#         cwd= "/opt/airflow/walmart_project",
#         bash_command="dbt test --select path:tests/test_obt.sql"
#     )

#     gold_ephermeral = BashOperator(
#         task_id = "gold_ephermeral",
#         cwd = "/opt/airflow/walmart_project",
#         bash_command="dbt run --select path:models/gold/ephemeral"
#     )


#     gold_dimensions = BashOperator(
#         task_id="gold_dimensions",
#         cwd="/opt/airflow/walmart_project",
#         bash_command="dbt snapshot"
#     )

#     gold_facts = BashOperator(
#         task_id="gold_facts",
#         cwd="/opt/airflow/walmart_project",
#         bash_command="dbt run --select path:models/gold/fact"
#     )
    
    


    # clean_target() >> source_freshness() >> silver_postgresql >> silver_api >> silver_s3
    clean = clean_target()
    freshness = source_freshness()

    # Task dependencies
    clean >> freshness >> [silver_postgresql, silver_api, silver_s3]


    clean >> freshness >> [
        silver_postgresql >> [silver_postgresql_test], 
        silver_api >> [silver_api_test], 
        silver_s3 >> [silver_s3_test]
    ]



orchestrate_dag = orchestrate()