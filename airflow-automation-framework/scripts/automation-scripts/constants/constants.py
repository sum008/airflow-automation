from enum import Enum


class Constants(Enum):
    COLLECTION_AIRFLOW = "airflow"
    JOB_METADATA_AIRFLOW = "pipeline_config"
    JOB_WORKFLOW_AIRFLOW = "pipeline_workflow"
    PARAMS_AIRFLOW = "params"
    PYTHON_CALLABLE_CLASSPATH_AIRFLOW = "python_callable_class_path"
    CLASSNAME_AIRFLOW = "class_name"
    PYTHON_CALLABLE_FUNCTION_AIRFLOW = "python_callable_function"
    PYTHON_CALLABLE_PATH_AIRFLOW = "python_callable_path"
    CALLABLE_PARAMS_AIRFLOW = "callable_params"
    TASK_TYPE_AIRFLOW = "task_type"
    CLASS_PARAM_AIRFLOW = "class_param"
    TASK_PARAMS_AIRFLOW = "task_params"
    DAG_PARAMS_AIRFLOW = "dag_params"
    PACKAGE_NAME = "master_refresh"
    CALLBACK_META_AIRFLOW = "callback_metadata"
    SLACK_FAILURE_CHANNEL = "https://hooks.slack.com/services/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"

