import json
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum


DBT_DIR = os.getenv("DBT_DIR")


def load_manifest(file: str) -> dict:
    """
    Reads the json `file` and returns it as a dict.
    """
    with open(file) as f:
        data = json.load(f)
    return data


def make_dbt_task(node: str, dbt_verb: str, target: str) -> BashOperator:
    """
    Returns a BashOperator with a bash command to run or test the given node.
    Adds the project-dir argument and names the tasks as shown by the below examples.
    Cleans the node's name when it is a test.

    Examples:
    >>> print(make_dbt_task('model.dbt_lewagon.my_first_dbt_model', 'run'))
    BashOperator(
        task_id=model.dbt_lewagon.my_first_dbt_model,
        bash_command= "dbt run --models my_first_dbt_model --project-dir /app/airflow/dbt_lewagon"
    )

    >>> print(make_dbt_task('test.dbt_lewagon.not_null_my_first_dbt_model_id.5fb22c2710', 'test'))
    BashOperator(
        task_id=test.dbt_lewagon.not_null_my_first_dbt_model_id,
        bash_command= "dbt test --models not_null_my_first_dbt_model_id --project-dir /app/airflow/dbt_lewagon"
    )
    """
    model_name = node.split(".")[-1] if dbt_verb == "run" else node.split(".")[-2]
    task_name = node if dbt_verb == "run" else ".".join(node.split(".")[:-1])
    return BashOperator(
        task_id=task_name,
        bash_command=f"dbt {dbt_verb} --models {model_name} --project-dir {DBT_DIR} --target {target}",
    )


def create_tasks(data: dict) -> dict:
    """
    This function should iterate through data["nodes"] keys and call make_dbt_task
    to build and return a new dict containing as keys all nodes' names and their corresponding dbt tasks as values.
    """
    res = {}
    for node_name, node_content in data["nodes"].items():
        dbt_verb = "test" if node_content["resource_type"] == "test" else "run"
        target = "dev_silver" if "silver" in node_content["original_file_path"] else "dev_gold"
        res[node_name] = make_dbt_task(node_name, dbt_verb, target)

    return res


def create_dags_dependencies(data: dict, dbt_tasks: dict):
    """
    Iterate over every node and their dependencies (by using data and the "depends_on" key)
    to order the Airflow tasks properly.
    """
    tasks_to_link_to_weather = []
    for node_name, node_content in data["nodes"].items():
        depended_on_nodes = []
        for depended_node in node_content["depends_on"]["nodes"]:
                depended_node_model_name = f"model.restaurant_dbt.{depended_node.split('.')[-1]}"
                if depended_node_model_name in data["nodes"]:
                    depended_on_nodes.append(depended_node_model_name)
                    dbt_tasks[depended_node_model_name] >> dbt_tasks[node_name]
                    
        if not depended_on_nodes and "silver" in node_content["original_file_path"]:
            tasks_to_link_to_weather.append(dbt_tasks[node_name])

    return tasks_to_link_to_weather

