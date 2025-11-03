from bytecodeairflow.operator_helper.core import Core
from bytecodeairflow.common_helper.helper import create_operator, load_config, load_attribute_from_package, \
    load_attribute_from_package_class_path
from bytecodeairflow.constants.constants import Constants


class OperatorCore(Core):
    def __init__(self):
        self.task_config = load_config(package_name="bytecodeairflow", filename="configurations/task_config.json",
                                       extension="json")

        super().__init__()

    def create_emr_job_flow(self, task):
        return self.load_param_to_emr_task("job_flow_overrides_key", "job_flow_overrides_path", "job_flow_overrides",
                                           task)

    def add_emr_step(self, task):
        return self.load_param_to_emr_task("steps_key", "steps_path", "steps", task)

    def load_param_to_emr_task(self, key1, key2, key3, task):
        if key1 in task and key2 in task:
            task[Constants.PARAMS_AIRFLOW.value][key3] = load_config(package_name=Constants.PACKAGE_NAME.value, filename=task[key2])[task[key1]]
            return create_operator(task_config=self.task_config,
                                   operator_name=task[Constants.TASK_TYPE_AIRFLOW.value],
                                   operator_params=task[Constants.PARAMS_AIRFLOW.value])
        else:
            return self.process_default(task)

    def process_default(self, task):
        return create_operator(task_config=self.task_config, operator_name=task[Constants.TASK_TYPE_AIRFLOW.value],
                               operator_params=task[Constants.PARAMS_AIRFLOW.value])

    # def process_python_operator(self, task):
    #     class_param_dict = task[Constants.CLASS_PARAM_AIRFLOW.value] if Constants.CLASS_PARAM_AIRFLOW.value in task else {}
    #     task[Constants.PARAMS_AIRFLOW.value]['python_callable'] = load_attribute_from_class_path(
    #         module_path=task[Constants.PYTHON_CALLABLE_CLASSPATH_AIRFLOW.value],
    #         class_name=task[Constants.CLASSNAME_AIRFLOW.value],
    #         attribute_name=task[Constants.PYTHON_CALLABLE_FUNCTION_AIRFLOW.value],
    #         class_params=class_param_dict) if Constants.PYTHON_CALLABLE_CLASSPATH_AIRFLOW.value in task else load_attribute_from_script(
    #         module_path=task[Constants.PYTHON_CALLABLE_PATH_AIRFLOW.value],
    #         attribute_name=task[Constants.PYTHON_CALLABLE_FUNCTION_AIRFLOW.value])
    #     if Constants.CALLABLE_PARAMS_AIRFLOW.value in task:
    #         task[Constants.PARAMS_AIRFLOW.value]["op_kwargs"] = {"args": task[Constants.CALLABLE_PARAMS_AIRFLOW.value]}
    #     return create_operator(task_config=self.task_config, operator_name=task[Constants.TASK_TYPE_AIRFLOW.value], operator_params=task[Constants.PARAMS_AIRFLOW.value])

    # def process_python_operator(self, task, create_operator, dag_obj):
    #     class_param_dict = task[
    #         Constants.CLASS_PARAM_AIRFLOW.value] if Constants.CLASS_PARAM_AIRFLOW.value in task else {}
    #     task[Constants.PARAMS_AIRFLOW.value]['python_callable'] = load_attribute_from_package_class_path(
    #         package="unifiedcloud",
    #         module_path=task[Constants.PYTHON_CALLABLE_CLASSPATH_AIRFLOW.value],
    #         class_name=task[Constants.CLASSNAME_AIRFLOW.value],
    #         attribute_name=task[Constants.PYTHON_CALLABLE_FUNCTION_AIRFLOW.value],
    #         class_params=class_param_dict) if Constants.PYTHON_CALLABLE_CLASSPATH_AIRFLOW.value in task else load_attribute_from_package(
    #         package="y",
    #         module_path=task[Constants.PYTHON_CALLABLE_PATH_AIRFLOW.value],
    #         attribute_name=task[Constants.PYTHON_CALLABLE_FUNCTION_AIRFLOW.value])
    #     if Constants.CALLABLE_PARAMS_AIRFLOW.value in task:
    #         task[Constants.PARAMS_AIRFLOW.value]["op_kwargs"] = {
    #             "args": task[Constants.CALLABLE_PARAMS_AIRFLOW.value]}
    #     if create_operator:

    #     return create_operator(task_config=self.task_config, operator_name=task[Constants.TASK_TYPE_AIRFLOW.value],
    #                            operator_params=task[Constants.PARAMS_AIRFLOW.value])

    def python_operator_integrity_check(self, task):
        class_param_dict = task[
            Constants.CLASS_PARAM_AIRFLOW.value] if Constants.CLASS_PARAM_AIRFLOW.value in task else {}
        task[Constants.PARAMS_AIRFLOW.value]['python_callable'] = load_attribute_from_package_class_path(
            package=task.get("package_name", Constants.PACKAGE_NAME.value),
            module_path=task[Constants.PYTHON_CALLABLE_CLASSPATH_AIRFLOW.value],
            class_name=task[Constants.CLASSNAME_AIRFLOW.value],
            attribute_name=task[Constants.PYTHON_CALLABLE_FUNCTION_AIRFLOW.value],
            class_params=class_param_dict) if Constants.PYTHON_CALLABLE_CLASSPATH_AIRFLOW.value in task else load_attribute_from_package(
            package=task.get("package_name", Constants.PACKAGE_NAME.value),
            module_path=task[Constants.PYTHON_CALLABLE_PATH_AIRFLOW.value],
            attribute_name=task[Constants.PYTHON_CALLABLE_FUNCTION_AIRFLOW.value])
        if Constants.CALLABLE_PARAMS_AIRFLOW.value in task:
            task[Constants.PARAMS_AIRFLOW.value]["op_kwargs"] = {
                Constants.CALLABLE_PARAMS_AIRFLOW.value: task[Constants.CALLABLE_PARAMS_AIRFLOW.value]}
        return task

    def process_python_operator(self, task):
        return create_operator(task_config=self.task_config, operator_name=task[Constants.TASK_TYPE_AIRFLOW.value],
                               operator_params=task[Constants.PARAMS_AIRFLOW.value])
