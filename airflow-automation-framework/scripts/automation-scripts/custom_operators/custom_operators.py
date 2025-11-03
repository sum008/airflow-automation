import json

from airflow.models import BaseOperator, DagBag
from airflow.utils.state import State
from airflow.exceptions import AirflowException
from airflow.utils.session import create_session
from airflow.api.common.trigger_dag import trigger_dag
from airflow.models import DagRun
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils import timezone
from airflow.utils.operator_helpers import determine_kwargs
from airflow.utils.session import provide_session
from airflow.utils.types import DagRunType
import time
import typing as t

class TriggerAndWaitForDAGOperator(BaseOperator):
    def __init__(self, child_dag_id, wait_for_completion=False, conf: dict | None = None, poke_interval=60, timeout=3600, *args, **kwargs):
        """
            This custom operator is a workaround of airflow provided TriggerDagRunOperator due to an existing bug in case of wait_for_completion=True
        """
        super(TriggerAndWaitForDAGOperator, self).__init__(*args, **kwargs)
        self.child_dag_id = child_dag_id
        self.wait_for_completion = wait_for_completion
        self.conf = conf
        self.poke_interval = poke_interval
        self.timeout = timeout

    def execute(self, context):
        try:
            json.dumps(self.conf)
        except TypeError:
            raise AirflowException("conf parameter should be JSON Serializable")

        dagbag = DagBag()
        child_dag = dagbag.get_dag(self.child_dag_id)

        if not child_dag:
            raise AirflowException(f"DAG {self.child_dag_id} not found in DagBag.")

        execution_date = context['execution_date']
        # formatted_execution_date = execution_date.strftime('%Y-%m-%dT%H:%M:%S.%f%z')

        # run_id = f"manual__{formatted_execution_date}"
        run_id = context['dag_run'].run_id
        if not run_id:
            run_id = DagRun.generate_run_id(DagRunType.MANUAL, execution_date)

        self.log.info(f"Triggering DAG {self.child_dag_id} with run_id: {run_id}")

        # Create a new DagRun for the child DAG
        with create_session() as session:
            try:
                child_dag_run = trigger_dag(
                    dag_id=self.child_dag_id,
                    run_id=run_id,
                    conf=self.conf,
                    # conf=context['dag_run'].conf,
                    execution_date=execution_date,
                    replace_microseconds=False,
                )

                session.add(child_dag_run)
                session.commit()

                if self.wait_for_completion:
                    self.log.info(f"Waiting for DAG {self.child_dag_id} to complete.")
                    start_time = time.time()
                    while True:
                        time.sleep(self.poke_interval)
                        elapsed_time = time.time() - start_time
                        self.log.info(f'CURRENT STATE IS :: {child_dag_run.state}')
                        child_dag_run.refresh_from_db()
                        if child_dag_run.state != State.RUNNING:
                            if child_dag_run.state == State.SUCCESS:
                                self.log.info(f"DAG {self.child_dag_id} has completed successfully.")
                                break
                            elif child_dag_run.state == State.FAILED:
                                raise AirflowException(f"DAG {self.child_dag_id} did not complete successfully. State: {child_dag_run.state}")

                        if elapsed_time > self.timeout:
                            raise AirflowException(f"Timeout: DAG {self.child_dag_id} did not complete in {self.timeout} seconds.")
                        self.log.info(f"DAG {self.child_dag_id} is still running. Waiting...")

            except Exception as e:
                self.log.exception("Error occurred while triggering and waiting for DAG.")
                raise AirflowException(f"Error occurred while triggering and waiting for DAG: {str(e)}")


class TriggerMultiDagRunOperator(TriggerDagRunOperator):
    # If a user wants to pass conf in operator params, then user have to pass the conf in operator params something
    # like conf={"param1":"value"},
    # this should be a dict only(json format) or None as we are adding this dict to the
    # dict we got from python_callable return
    def __init__(self, op_args=None, op_kwargs=None, python_callable=None, conf: dict | None = None, *args, **kwargs):
        super(TriggerMultiDagRunOperator, self).__init__(*args, **kwargs)
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}
        self.python_callable = python_callable
        self.conf = conf

    @provide_session
    def execute(self, context: t.Dict, session=None):
        try:
            json.dumps(self.conf)
        except TypeError:
            raise AirflowException("conf parameter should be JSON Serializable")

        context.update(self.op_kwargs)
        self.op_kwargs = determine_kwargs(self.python_callable, self.op_args, context)

        # created_dr_ids = []
        for return_of_python_callable in self.python_callable(*self.op_args, **self.op_kwargs):
            if not return_of_python_callable:
                break
            if self.conf:
                return_of_python_callable.update(self.conf)
            execution_date = timezone.utcnow()

            # run_id = conf.get('run_id')
            # if not run_id:

            # we need each run_id of child dag to be different
            run_id = DagRun.generate_run_id(DagRunType.MANUAL, execution_date)

            # Theoretically, we can remove this line since we are using timezone.utcnow() to create run_id,
            # but it's making this operator more fail-safe
            dag_run = DagRun.find(dag_id=self.trigger_dag_id, run_id=run_id)
            if not dag_run:
                dag_run = trigger_dag(
                    dag_id=self.trigger_dag_id,
                    run_id=run_id,
                    conf=return_of_python_callable,
                    execution_date=execution_date,
                    replace_microseconds=False,
                )
                self.log.info("Created DagRun %s, %s - %s", dag_run, self.trigger_dag_id, run_id)
            else:
                dag_run = dag_run[0]
                self.log.warning("Fetched existed DagRun %s, %s - %s", dag_run, self.trigger_dag_id, run_id)
            # time.sleep(5)
            # created_dr_ids.append(dag_run.id)

        # if created_dr_ids:
        #     xcom_key = get_multi_dag_run_xcom_key(context['execution_date'])
        #     context['ti'].xcom_push(xcom_key, created_dr_ids)
        #     self.log.info("Pushed %s DagRun's ids with key %s", len(created_dr_ids), xcom_key)
        # else:
        #     self.log.info("No DagRuns created")


class TriggerMultiDagRunWithWaitOperator(TriggerDagRunOperator):
    # If a user wants to pass conf in operator params, then user have to pass the conf in operator params something
    # like conf={"param1":"value"},
    # this should be a dict only(json format) or None as we are adding this dict to the
    # dict we got from python_callable return
    def __init__(self, op_args=None, op_kwargs=None, python_callable=None, poke_interval=60, timeout=3600, conf: dict | None = None, *args, **kwargs):
        super(TriggerMultiDagRunWithWaitOperator, self).__init__(*args, **kwargs)
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}
        self.python_callable = python_callable
        self.conf = conf
        self.poke_interval = poke_interval
        self.timeout = timeout

    @provide_session
    def execute(self, context: t.Dict, session=None):
        try:
            json.dumps(self.conf)
        except TypeError:
            raise AirflowException("conf parameter should be JSON Serializable")

        context.update(self.op_kwargs)
        self.op_kwargs = determine_kwargs(self.python_callable, self.op_args, context)

        run_ids = []
        for return_of_python_callable in self.python_callable(*self.op_args, **self.op_kwargs):
            if not return_of_python_callable:
                break
            if self.conf:
                return_of_python_callable.update(self.conf)
            execution_date = timezone.utcnow()

            # run_id = conf.get('run_id')
            # if not run_id:

            # we need each run_id of child dag to be different
            run_id = DagRun.generate_run_id(DagRunType.MANUAL, execution_date)

            # Theoretically, we can remove this line since we are using timezone.utcnow() to create run_id,
            # but it's making this operator more fail-safe
            dag_run = DagRun.find(dag_id=self.trigger_dag_id, run_id=run_id)
            if not dag_run:
                dag_run = trigger_dag(
                    dag_id=self.trigger_dag_id,
                    run_id=run_id,
                    conf=return_of_python_callable,
                    execution_date=execution_date,
                    replace_microseconds=False,
                )
                run_ids.append(dag_run)
                self.log.info("Created DagRun %s, %s - %s", dag_run, self.trigger_dag_id, run_id)
            else:
                run_ids.append(dag_run)
                dag_run = dag_run[0]
                self.log.warning("Fetched existed DagRun %s, %s - %s", dag_run, self.trigger_dag_id, run_id)

        if self.wait_for_completion:
            self.log.info(f"Waiting for DAG {self.trigger_dag_id} to complete.")
            start_time = time.time()
            while len(run_ids) > 0:
                index = 0
                time.sleep(self.poke_interval)
                elapsed_time = time.time() - start_time
                while index < len(run_ids):
                    child_dag_run = run_ids[index]
                    self.log.info(f'CURRENT STATE FOR :: {child_dag_run.id} IS :: {child_dag_run.state}')
                    child_dag_run.refresh_from_db()
                    if child_dag_run.state != State.RUNNING:
                        if child_dag_run.state == State.SUCCESS:
                            self.log.info(f"DAG {child_dag_run.id} has completed successfully.")
                            run_ids.remove(run_ids[index])
                        elif child_dag_run.state == State.FAILED:
                            raise AirflowException(
                                f"DAG {self.trigger_dag_id} - {child_dag_run} did not complete successfully. State: {child_dag_run.state}")
                    index += 1
                    time.sleep(0.5)
                if len(run_ids) == 0:
                    self.log.info(f"DAG {self.trigger_dag_id} finished in {elapsed_time}...")
                else:
                    if elapsed_time > self.timeout:
                        raise AirflowException(
                            f"Timeout: DAG {self.trigger_dag_id} did not complete in {self.timeout} seconds.")
                    self.log.info(f"DAG {self.trigger_dag_id} is still running. Waiting...")
