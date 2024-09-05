from pendulum import datetime
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig
from pathlib import Path

DBT_PATH = "/usr/local/airflow/dags/dbt"
DBT_PROFILE = "dbt_project"
DBT_TARGETS = "dev"


profile_config = ProfileConfig(
    profile_name=DBT_PROFILE,
    target_name=DBT_TARGETS,
    profiles_yml_filepath=Path(f'{DBT_PATH}/profiles.yml')
)

# Models dbt
project_config = ProjectConfig(
    dbt_project_path=DBT_PATH,
    models_relative_path="models"
)

@dag(
    start_date=datetime(2024,8,26),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["dbt", "snowflake"],
)
def dbt_snowflake():


    init = EmptyOperator(task_id="init")

    dbt_running_models = DbtTaskGroup(
        group_id="dbt_running_models",
        project_config=project_config,
        profile_config=profile_config,
        default_args={"retries": 2},
    )

    finish = EmptyOperator(task_id="finish"
    )

    init >> dbt_running_models>> finish

dag = dbt_snowflake()

