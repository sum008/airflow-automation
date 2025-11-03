from bytecodeairflow.common_helper.helper import load_attribute_from_package
from bytecodeairflow.operator_helper.operator_core import OperatorCore
from bytecodeairflow.constants.constants import Constants
import logging


class TaskInitializer:

    def __init__(self):
        self.operator_utils = OperatorCore()
        self.logger = logging.Logger(__name__)
        self.logger.setLevel(logging.INFO)

    def task_integrity_check(self, task):
        match task[Constants.TASK_TYPE_AIRFLOW.value]:
            case "python_operator" | "branch_python_operator" | "trigger_multi_dag" | "short_circuit_operator" | "trigger_multi_dag_with_wait":
                return self.operator_utils.python_operator_integrity_check(task)
            case _:
                self.logger.info(f'Invalid operator type :: {task[Constants.TASK_TYPE_AIRFLOW.value]} for task_integrity_check, currently task_integrity_check only supports :: [python_operator, branch_python_operator], returning same task...')
                return task

    def initialize_task(self, task):
        match task[Constants.TASK_TYPE_AIRFLOW.value]:
            case "python_operator" | "branch_python_operator" | "trigger_multi_dag" | "short_circuit_operator" | "trigger_multi_dag_with_wait":
                return self.operator_utils.process_python_operator(task)
            case "emr_create_job_flow_operator":
                return self.operator_utils.create_emr_job_flow(task)
            case "emr_add_steps_operator":
                return self.operator_utils.add_emr_step(task)
            case _:
                return self.operator_utils.process_default(task)

    # dag_param[key] can have only one on_success_callback and on_failure_callback,
    # and default_args can have only one on_success_callback and on_failure_callback
    def add_callback_to_dag_params(self, dag_param: dict, callback_metadata: dict):
        for key, value in callback_metadata.items():
            if isinstance(value, list):
                for callback in value:
                    self.add_callback_to_param(callback=callback, dag_param=dag_param, key=key)
            else:
                self.add_callback_to_param(callback=value, dag_param=dag_param, key=key)
        return dag_param

    def add_callback_to_param(self, callback, dag_param, key):
        loaded_attribute = load_attribute_from_package(
            package=callback['package_name'],
            module_path=callback['python_callable_path'],
            attribute_name=callback['python_callable_function'])
        if callback.get("default_args"):
            if "default_args" in dag_param:
                dag_param['default_args'][key] = loaded_attribute
            else:
                dag_param['default_args'] = {key: loaded_attribute}
        else:
            dag_param[key] = loaded_attribute
