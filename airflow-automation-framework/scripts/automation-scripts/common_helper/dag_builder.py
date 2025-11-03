import logging

from airflow import DAG
from airflow.models import Variable
from bytecodeairflow.common_helper.task_initializer import TaskInitializer
from bytecodeairflow.constants.constants import Constants
from bytecodeairflow.pipeline_meta_fetcher.mongodb_helper import MongodbConnector


class AirflowDagBuilder:

    def __init__(self):
        self.callback_metadata = None
        self.pipeline_id = None
        self.dag_params = None
        self.task_list = None
        self.workflow = None
        self.dag = None
        self.task_initializer = TaskInitializer()
        self.mongo_connector = MongodbConnector(Variable.get('mongodb_sec'), Variable.get('sec_region'))
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

    def build_airflow_dag_object(self, pipeline_id: str):
        self.logger.info(f'STARTING DAG PREPARATION FOR PIPELINE_ID :: {pipeline_id}')
        return (self.initialize_builder_params(pipeline_id)
                .add_callback_if_needed()
                .check_task_integrity_for_workflow()
                .prepare_dag_object()
                .dag)

    def initialize_builder_params(self, pipeline_id):
        self.pipeline_id = pipeline_id
        pipeline_meta = self.mongo_connector.fetch_metadata(Constants.JOB_METADATA_AIRFLOW.value,pipeline_id)
        self.workflow = self.mongo_connector.fetch_metadata(Constants.JOB_WORKFLOW_AIRFLOW.value,pipeline_id)["workflow"]
        self.task_list = pipeline_meta[Constants.TASK_PARAMS_AIRFLOW.value]
        self.dag_params = pipeline_meta[Constants.DAG_PARAMS_AIRFLOW.value]
        self.callback_metadata = pipeline_meta.get(Constants.CALLBACK_META_AIRFLOW.value, {})
        return self

    def add_callback_if_needed(self):
        if self.callback_metadata:
            try:
                self.dag_params = self.task_initializer.add_callback_to_dag_params(self.dag_params,
                                                                                       self.callback_metadata)
            except Exception as err:
                exception_msg = f"Exception occurred while adding callback for pipeline :: {self.pipeline_id} due to error :: {err}"
                self.logger.error(exception_msg)
                raise Exception(exception_msg)
        return self

    def check_task_integrity_for_workflow(self):
        try:
            for index in range(len(self.task_list)):
                self.task_list[index] = self.task_initializer.task_integrity_check(self.task_list[index])
        except Exception as err:
            exception_msg = f"Exception occurred while task integrity check for pipeline :: {self.pipeline_id} due to error :: {err}"
            self.logger.error(exception_msg)
            raise Exception(exception_msg)
        return self

    def prepare_dag_object(self):
        try:
            with DAG(**self.dag_params) as dag:
                tasks = {}
                for task in self.task_list:
                    tasks[task["params"]["task_id"]] = self.task_initializer.initialize_task(task)

                # it could be possible that there is a list of workflows, so we have to put the layout of all those
                # workflows, otherwise process the single workflow
                if isinstance(self.workflow, list):
                    for current_workflow in self.workflow:
                        self.workflow_layout(tasks=tasks, current_workflow=current_workflow)
                else:
                    self.workflow_layout(tasks=tasks, current_workflow=self.workflow)
            self.logger.info(f'FINISHED DAG PREPARATION FOR PIPELINE_ID :: {self.pipeline_id}, RETURNING DAG...')
            self.dag = dag
        except Exception as err:
            raise err
        return self

    def workflow_layout(self, tasks, current_workflow):
        for key, value in current_workflow.items():
            if isinstance(value, list):
                for cur_task_id in value:
                    tasks[key] >> tasks[cur_task_id]
            else:
                tasks[key] >> tasks[value]
