import time
import logging
import json
import requests
from install_build import main
from airflow.models import Variable

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

env = Variable.get('env')
def send_notification(url,payload):
    time.sleep(0.5)
    slack_payload = {'type': 'mrkdwn', 'text': payload}
    response = requests.post(
        url,
        data=json.dumps(slack_payload),
        headers={'Content-Type': 'application/json'}
    )
    if response.status_code != 200:
        logger.error("Unable to send slack notification")

main()

from bytecodeairflow.common_helper.dag_builder import AirflowDagBuilder
from bytecodeairflow.pipeline_meta_fetcher.mongodb_helper import MongodbConnector

from bytecodeairflow.constants.constants import Constants

mongo_connector = MongodbConnector(Variable.get('mongodb_sec'), Variable.get('sec_region'))

pipeline_ids = mongo_connector.fetch_metadata("pipeline_ids","pipeline_ids")[
        'pipeline_list']



logger.info(f'List of pipeline_ids :: {pipeline_ids}')

dag_builder = AirflowDagBuilder()

def generate_dags():
    slack_url = Variable.get("slack_dag_build_status_channel")

    for pipeline_id_dict in pipeline_ids:
        if next(iter(pipeline_id_dict.values())):
            pipeline_id = next(iter(pipeline_id_dict.keys()))
            try:
                logger.info(f'Building DAG  :: {pipeline_id}')
                dag_builder.build_airflow_dag_object(pipeline_id=pipeline_id)
                logger.info(f"Sucessfully built dag {pipeline_id}")
            except Exception as err:
                logger.error(f'Error occurred while building dag object for pipeline_id :: {pipeline_id}, '
                             f'\n  {err}')
                payload = f"Failed to build dag {pipeline_id} , {err} \n env {env}"
                send_notification(slack_url,payload)


generate_dags()