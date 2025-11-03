import subprocess
from airflow import DAG

from bytecodeairflow.common_helper.dag_builder import AirflowDagBuilder
from datetime import timedelta
import datetime
import datetime as dtm

from pymongo import MongoClient


class MongodbConnector():
    def __init__(self, url):
        self.mongo_connection = self.connect_to_mongodb(connection_url=url)

    def connect_to_mongodb(self, connection_url):
        return MongoClient(connection_url)

    def insert_metadata(self, data_to_insert, collection, document):
        return self.mongo_connection[collection][document].insert_one(data_to_insert)

    def update_metadata(self, *args, **kwargs):
        pass

    def fetch_metadata(self, collection, document, pipeline_id):
        return self.mongo_connection[collection][document].find_one(filter={"_id": pipeline_id})

    def delete_metadata(self, collection, document, pipeline_id):
        return self.mongo_connection[collection][document].delete_one({'_id': pipeline_id})

    def replace_metadata(self, collection, document, pipeline_id, new_metadata):
        return self.mongo_connection[collection][document].replace_one({"_id": pipeline_id}, new_metadata)


def python_task1(**kwargs):
    print(F"YES! THIS TASK python_task1 HAS BEEN CALLED WITH PARAMS {kwargs['args']}...")


def python_task2(**kwargs):
    print(F"YES! THIS TASK python_task2 HAS BEEN CALLED WITH PARAMS {kwargs}...")


def insert_to_mongo():
    # shell_commands = (
    #     'wget https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem'
    # )
    #
    # subprocess.check_call(shell_commands, shell=True)
    # url = "mongodb://sumit:sumit123@docdb-2024-06-26-08-32-24.cluster-cduks4mscct2.us-east-2.docdb.amazonaws.com:27017/?tls=true&tlsCAFile=global-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"
    # mongo_connector = MongodbConnector(
    #     url="mongodb://sumit1:root@127.0.0.1:27017/"
    #     )

    test_pipeline_config = {"_id": "test_pipeline",
                            "dag_params": {"dag_id": "test_pipeline",
                                           "schedule": None,
                                           "start_date": datetime.datetime.now(tz=datetime.timezone.utc)
                                           },
                            "insert_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
                            "task_params": [
                                {"task_type": "dummy_operator",
                                 "params": {"task_id": "t1"}
                                 },
                                {"task_type": "dummy_operator",
                                 "params": {"task_id": "t2"}
                                 },
                                {"task_type": "dummy_operator",
                                 "params": {"task_id": "t3"}
                                 },
                                {"task_type": "dummy_operator",
                                 "params": {"task_id": "t4"}
                                 }
                            ]
                            }

    test_pipeline_workflow = {"_id": "test_pipeline",
                              "workflow": {"t1": "t2",
                                           "t2": ["t3", "t4"]}}

    # res1 = mongo_connector.insert_metadata(data_to_insert=test_pipeline_config, collection="airflow",
    #                                        document="pipeline_config")
    # res2 = mongo_connector.insert_metadata(data_to_insert=test_pipeline_workflow, collection="airflow",
    #                                        document="pipeline_workflow")

    # print(res1, res2)

    params = {
        "config_path": "ck_pipelines/business_unit/auto_analytics/configs/dev/config.json",
        "bucket_path": "bucket_json/buckets.json",
        "month": 6,
        "year": 2024,
        "app": "auto",
        "env": "dev",
        "payer": "741843927392",
        "staging_bucket": "ck-data-pipeline-stage-bucket-airflow-poc",
        "config_bucket": "airflow-config-bucket",
        "template": "payer",
        "output_bucket": "airflow-config-bucket",
        "families": "",
        "core_node_instance_type": "m6g.8xlarge",
        "task_node_instance_type": "m6g.8xlarge",
        "core_node_instance_count": 30,
        "task_node_instance_count": 10,
        "core_node_spot_percent": 30,
        "context": "ckAutoContext",
        "source_bucket": "airflow-config-bucket"
    }

    auto_analytics_phase1 = {"_id": "auto_analytics_phase1",
                             "dag_params": {"dag_id": "auto_analytics_phase1",
                                            "schedule": '13 10 * * *',
                                            "params": params,
                                            "catchup": False,
                                            "start_date": dtm.datetime.now(tz=dtm.timezone.utc) - timedelta(days=1)
                                            },
                             "insert_timestamp": dtm.datetime.now(tz=dtm.timezone.utc),
                             "task_params": [
                                 {"task_type": "python_operator",
                                  "python_callable_path": "ck_pipelines/business_unit/shared_helpers/set_global_args.py",
                                  "python_callable_function": "set_global_args",
                                  "callable_params": {
                                      "config_path": "ck_pipelines/business_unit/auto_analytics/configs/dev/config.json"
                                  },
                                  "params": {"task_id": "set_global_args"}
                                  },
                                 {"task_type": "python_operator",
                                  "python_callable_path": "ck_pipelines/business_unit/auto_analytics/workflow_component/update_bucket_json.py",
                                  "python_callable_function": "main",
                                  "callable_params": {
                                      "config_path": "ck_pipelines/business_unit/auto_analytics/configs/dev/config.json",
                                      "bucket_path": "ck_pipelines/business_unit/auto_analytics/configs/buckets.json"
                                  },
                                  "params": {"task_id": "update_bucket_json"}
                                  },
                                 {"task_type": "python_operator",
                                  "python_callable_path": "ck_pipelines/business_unit/auto_analytics/workflow_component/retrieve_filter_payers.py",
                                  "python_callable_function": "main",
                                  "callable_params": {
                                      "config_path": "ck_pipelines/business_unit/auto_analytics/configs/dev/config.json",
                                      "bucket_path": "ck_pipelines/business_unit/auto_analytics/configs/buckets.json"
                                  },
                                  "params": {"task_id": "fetch_and_filter_payers"}
                                  },
                                 {"task_type": "python_operator",
                                  "python_callable_path": "ck_pipelines/business_unit/auto_analytics/workflow_component/manifest_and_data_file.py",
                                  "python_callable_function": "main",
                                  "callable_params": {
                                      "config_path": "ck_pipelines/business_unit/auto_analytics/configs/dev/config.json",
                                      "bucket_path": "ck_pipelines/business_unit/auto_analytics/configs/buckets.json"
                                  },
                                  "params": {"task_id": "filter_manifest_and_data_files"}
                                  },
                                 {"task_type": "python_operator",
                                  "python_callable_path": "ck_pipelines/business_unit/auto_analytics/workflow_component/data_validation.py",
                                  "python_callable_function": "main",
                                  "callable_params": {
                                      "config_path": "ck_pipelines/business_unit/auto_analytics/configs/dev/config.json",
                                      "bucket_path": "ck_pipelines/business_unit/auto_analytics/configs/buckets.json"
                                  },
                                  "params": {"task_id": "validate_data_files"}
                                  },
                                 {"task_type": "python_operator",
                                  "python_callable_path": "ck_pipelines/business_unit/auto_analytics/workflow_component/clean_staging_data.py",
                                  "python_callable_function": "main",
                                  "callable_params": {
                                      "config_path": "ck_pipelines/business_unit/auto_analytics/configs/dev/config.json"
                                  },
                                  "params": {"task_id": "clean_data"}
                                  },
                                 {"task_type": "python_operator",
                                  "python_callable_path": "ck_pipelines/business_unit/auto_analytics/workflow_component/copy_intermediate_data.py",
                                  "python_callable_function": "main",
                                  "callable_params": {
                                      "config_path": "ck_pipelines/business_unit/auto_analytics/configs/dev/config.json"
                                  },
                                  "params": {"task_id": "copy_data"}
                                  },
                                 {"task_type": "python_operator",
                                  "python_callable_path": "ck_pipelines/business_unit/auto_analytics/workflow_component/emr_driver.py",
                                  "python_callable_function": "emr_processing",
                                  "callable_params": {
                                      "config_path": "ck_pipelines/business_unit/auto_analytics/configs/dev/config.json"
                                  },
                                  "params": {"task_id": "emr_processing"}
                                  },
                                 {"task_type": "python_operator",
                                  "python_callable_path": "ck_pipelines/business_unit/auto_analytics/workflow_component/terminate_emr_cluster.py",
                                  "python_callable_function": "terminate_cluster",
                                  "callable_params": {
                                      "config_path": "ck_pipelines/business_unit/auto_analytics/configs/dev/config.json"
                                  },
                                  "params": {"task_id": "terminate_emr_cluster"}
                                  },
                                 {"task_type": "python_operator",
                                  "python_callable_path": "ck_pipelines/business_unit/auto_analytics/workflow_component/refresh_master.py",
                                  "python_callable_function": "master_refresh",
                                  "callable_params": {
                                      "config_path": "ck_pipelines/business_unit/auto_analytics/configs/dev/config.json"
                                  },
                                  "params": {"task_id": "refresh_master_snowflake"}
                                  }
                             ]
                             }

    auto_analytics_phase1_workflow = {"_id": "auto_analytics_phase1",
                                      "workflow": {"set_global_args": "update_bucket_json",
                                                   "update_bucket_json": "fetch_and_filter_payers",
                                                   "fetch_and_filter_payers": "filter_manifest_and_data_files",
                                                   "filter_manifest_and_data_files": "validate_data_files",
                                                   "validate_data_files": "clean_data",
                                                   "clean_data": "copy_data", "copy_data": "emr_processing",
                                                   "emr_processing": "terminate_emr_cluster",
                                                   "terminate_emr_cluster": "refresh_master_snowflake"}}

    child_dag_trigger_test = {"_id": "child_dag_trigger_test",
                              "dag_params": {"dag_id": "child_dag_trigger_test",
                                             "schedule": '46 10 * * *',
                                             "catchup": False,
                                             "start_date": dtm.datetime.now(tz=dtm.timezone.utc) - timedelta(days=1)
                                             },
                              "insert_timestamp": dtm.datetime.now(tz=dtm.timezone.utc),
                              "task_params": [
                                  {"task_type": "trigger_child_dag",
                                   "params": {"task_id": "trigger_and_wait_for_child_dag",
                                              "child_dag_id": "child_dag_id",
                                              "wait_for_completion": False,
                                              "poke_interval": 2,
                                              "timeout": 120}
                                   }
                              ]
                              }

    child_dag_trigger_test_workflow = {"_id": "child_dag_trigger_test",
                                       "workflow": {}}

    develop_test = {"_id": "develop_test",
                    "dag_params": {"dag_id": "develop_test",
                                   "schedule": None,
                                   "catchup": False,
                                   "start_date": dtm.datetime.now(tz=dtm.timezone.utc) - timedelta(days=1)
                                   },
                    "insert_timestamp": dtm.datetime.now(tz=dtm.timezone.utc),
                    "task_params": [
                        {"task_type": "python_operator",
                         "python_callable_class_path": "integrator/set_global_arguements.py",
                         "class_name": "GlobalArgumentsIntegrator",
                         "python_callable_function": "main",
                         "params": {"task_id": "set_global_arguments"}
                         },
                        {"task_type": "python_operator",
                         "python_callable_class_path": "integrator/fetch_payers_data.py",
                         "class_name": "FetchPayersInfoIntegrator",
                         "python_callable_function": "main",
                         "params": {"task_id": "fetch_payers_data"}
                         }
                    ]
                    }
    # "callback_metadata": {
    #     "on_success_callback": [{
    #         "package_name": "core_utils",
    #         "python_callable_path": "utils/store_task_info.py",
    #         "python_callable_function": "task_state_callback",
    #         "default_args": True
    #     }],
    #     "on_failure_callback": [{
    #         "package_name": "core_utils",
    #         "python_callable_path": "utils/store_task_info.py",
    #         "python_callable_function": "task_state_callback",
    #         "default_args": True
    #     },
    #         {
    #             "package_name": "core_utils",
    #             "python_callable_path": "utils/store_task_info.py",
    #             "python_callable_function": "just_second_callback",
    #             "default_args": False
    #         }
    #     ]
    # }
    callback_test = {
        "_id": "callback_test",
        "callback_metadata": {
            "on_success_callback": {
                "package_name": "core_utils",
                "python_callable_path": "utils/store_task_info.py",
                "python_callable_function": "task_state_callback",
                "default_args":True
            },
            "on_failure_callback": [{
                "package_name": "core_utils",
                "python_callable_path": "utils/store_task_info.py",
                "python_callable_function": "task_state_callback",
                "default_args": True
            },
                {
                    "package_name": "core_utils",
                    "python_callable_path": "utils/store_task_info.py",
                    "python_callable_function": "just_second_callback",
                    "default_args": False
                }
            ]
        },
        "dag_params": {
            "dag_id": "callback_test",
            "schedule": None,
           "catchup": False,
            "default_args":{'owner': 'sumit'},
           "start_date": dtm.datetime.now(tz=dtm.timezone.utc) - timedelta(days=1)
        },
        "insert_timestamp": dtm.datetime.now(tz=dtm.timezone.utc),
        "task_params": [
            {
                "task_type": "python_operator",
                "python_callable_class_path": "integrator/test.py",
                "class_name": "Test",
                "python_callable_function": "task1",
                "params": {
                    "task_id": "task1"
                }
            },
            {
                "task_type": "python_operator",
                "python_callable_class_path": "integrator/test.py",
                "class_name": "Test",
                "python_callable_function": "task2",
                "params": {
                    "task_id": "task2"
                }
            }
        ]
    }

    callback_test_workflow = {"_id": "callback_test",
                              "workflow": {"task1": "task2"}}

    refresh_master_dev_workflow = {"_id": "refresh_master_dev",
                                   "workflow": {"set_global_arguments": "fetch_payers_data"}}

    auto_20586 = {
        "_id": "ck-auto_20586",
        "ck-auto_20586": {
            "bucket_secret": "snowflake/airflow/ck-auto/cln_data_ck_poc/read_write",
            "emr_trigger_secret": "snowflake/airflow/ck-auto/cln_data_ck_poc/read_write",
            "mysql_secret": "cln-mysql-prod",
            "master_refresh_secret": "snowflake/airflow/ck-auto/cln_data_ck_poc/read_write",
            "s3_role": "arn:aws:iam::992382530041:role/EMRFullAccessJenkins",
            "emr_role": "arn:aws:iam::992382530041:role/EMRFullAccessJenkins",
            "last_refresh": 7,
            "api": "http://cloudonomic-ecs-prod-java-billdesk.cloudonomic-internal-route.com/billdesk/api/ck-auto/anonymous/customer-with-payer",
            "payer_bucket_schema": {
                "db": "CK_POC",
                "schema": "AIRFLOW_POC",
                "table": "payer_refresh_config"
            },
            "emr_trigger_schema": {
                "db": "CK_POC",
                "schema": "AIRFLOW_POC",
                "table": "ck_auto_cur_record"
            },
            "master_refresh": {
                "table": "ck_auto_cur",
                "storage_integration": "AWS_S3_BILLDESK",
                "stage_name": "aws_s3_ck_auto_master_non_prod_prod",
                "file_format": "ck_auto_prod.ck_auto_master.CSVMASTER"
            },
            "emr_context": "ckAutoContext",
            "emr_version": "emr-6.3.1",
            "emr_log_uri": "s3://aws-logs-992382530041-us-east-2/elasticmapreduce/",
            "app_tag": "pro",
            "config_bucket": "airflow-config-bucket",
            "staging_bucket": "",
            "output_bucket": ""
        }
    }

    payer_20586 = {
        "_id": "payer_20586",
        "payer_20586": {
            "bucket_secret": "snowflake/airflow/ck-auto/cln_data_ck_poc/read_write",
            "emr_trigger_secret": "snowflake/airflow/ck-auto/cln_data_ck_poc/read_write",
            "mysql_secret": "cln-mysql-prod",
            "master_refresh_secret": "snowflake/airflow/ck-auto/cln_data_ck_poc/read_write",
            "s3_role": "arn:aws:iam::992382530041:role/EMRFullAccessJenkins",
            "emr_role": "arn:aws:iam::992382530041:role/EMRFullAccessJenkins",
            "last_refresh": 7,
            "api": "http://cloudonomic-ecs-prod-java-billdesk.cloudonomic-internal-route.com/billdesk/api/ck-auto/anonymous/customer-with-payer",
            "payer_bucket_schema": {
                "db": "CK_POC",
                "schema": "AIRFLOW_POC",
                "table": "payer_refresh_config"
            },
            "emr_trigger_schema": {
                "db": "CK_POC",
                "schema": "AIRFLOW_POC",
                "table": "ck_auto_cur_record"
            },
            "master_refresh": {
                "table": "ck_auto_cur",
                "storage_integration": "AWS_S3_BILLDESK",
                "stage_name": "aws_s3_ck_auto_master_non_prod_prod",
                "file_format": "ck_auto_prod.ck_auto_master.CSVMASTER"
            },
            "emr_context": "ckAutoContext",
            "emr_version": "emr-6.3.1",
            "emr_log_uri": "s3://aws-logs-992382530041-us-east-2/elasticmapreduce/",
            "app_tag": "pro",
            "config_bucket": "airflow-config-bucket",
            "staging_bucket": "",
            "output_bucket": ""
        }
    }

    global_args_test = {"_id": "global_args_test",
                        "dag_params": {"dag_id": "global_args_test",
                                       "schedule": None,
                                       "catchup": False,
                                       "start_date": dtm.datetime.now(tz=dtm.timezone.utc) - timedelta(days=1)
                                       },
                        "insert_timestamp": dtm.datetime.now(tz=dtm.timezone.utc),
                        "task_params": [
                            {"task_type": "python_operator",
                             "python_callable_class_path": "integrator/set_global_args.py",
                             "class_name": "GlobalArgumentsIntegrator",
                             "python_callable_function": "main",
                             "params": {"task_id": "set_global_arguments"}
                             }
                        ]
                        }

    global_args_test_workflow = {"_id": "global_args_test",
                                 "workflow": {}}

    test_pipeline_workflow = {"_id": "test_pipeline",
                              "workflow": [{"t1": "t2", "t2": "t3", "t3": "t4", "t4": "t5", "t5": "t6"},
                                           {"t1": "t2", "t2": "t3", "t3": "t4", "t4": "t6"},
                                           {"t1": "t2", "t2": "t6"}]}

    refresh_master_dev_parakh = {
        "_id": "refresh_master_dev_parakh",
        "dag_params": {
            "dag_id": "refresh_master_dev_parakh",
            "schedule": None,
            "catchup": False,
            "start_date": dtm.datetime.now(tz=dtm.timezone.utc)
        },
        "insert_timestamp": dtm.datetime.now(tz=dtm.timezone.utc),
        "task_params": [
            {
                "task_type": "python_operator",
                "python_callable_class_path": "integrator/validate_and_set_global_args.py",
                "class_name": "ValidateAndSetGlobalArgumentsIntegrator",
                "python_callable_function": "main",
                "params": {
                    "task_id": "validate_and_set_global_arguments"
                }
            },
            {
                "task_type": "python_operator",
                "python_callable_class_path": "integrator/fetch_payers_data.py",
                "class_name": "FetchPayersInfoIntegrator",
                "python_callable_function": "main",
                "params": {
                    "task_id": "fetch_payers_data"
                }
            }
            ,
            {
                "task_type": "branch_python_operator",
                "python_callable_class_path": "integrator/branch_operator.py",
                "class_name": "DecisionMaker",
                "python_callable_function": "main",
                "params": {
                    "task_id": "workflow_decision_maker"
                }
            },
            {"task_type": "python_operator",
             "python_callable_class_path": "integrator/get_customer_info.py",
             "class_name": "GetCustomerInfoIntegrator",
             "python_callable_function": "main",
             "params": {"task_id": "get_customer_info"}
             },
            {"task_type": "python_operator",
             "python_callable_class_path": "integrator/get_max_timestamp_common.py",
             "class_name": "GetMaxTimestamp",
             "python_callable_function": "main",
             "params": {"task_id": "get_max_timestamp"}
             },
            {"task_type": "python_operator",
             "python_callable_class_path": "integrator/validate_and_filter_payers.py",
             "class_name": "ValidateAndFilterPayers",
             "python_callable_function": "main",
             "params": {"task_id": "validate_and_filter_payers", "trigger_rule": "none_failed"}
             },
            {"task_type": "python_operator",
             "python_callable_class_path": "integrator/check_and_update_db.py",
             "class_name": "CheckUpdateDBIntegrator",
             "python_callable_function": "main",
             "params": {"task_id": "check_update_db"}
             },
            {"task_type": "trigger_multi_dag",
             "python_callable_class_path": "integrator/dag_run_generator.py",
             "class_name": "GenerateDagRunIntegrator",
             "python_callable_function": "main",
             "params": {"task_id": "generate_dag_run", "trigger_dag_id": "refresh_master_dag2"}
             }
        ]
    }

    refresh_master_dev_parakh_workflow = {"_id": "refresh_master_dev_parakh",
                                          "workflow": [{"validate_and_set_global_arguments": "fetch_payers_data",
                                                        "fetch_payers_data": "workflow_decision_maker",
                                                        "workflow_decision_maker": "get_customer_info",
                                                        "get_customer_info": "get_max_timestamp",
                                                        "get_max_timestamp": "check_update_db",
                                                        "check_update_db": "validate_and_filter_payers",
                                                        "validate_and_filter_payers": "generate_dag_run"},
                                                       {"validate_and_set_global_arguments": "fetch_payers_data",
                                                        "fetch_payers_data": "workflow_decision_maker",
                                                        "workflow_decision_maker": "validate_and_filter_payers",
                                                        "validate_and_filter_payers": "generate_dag_run"
                                                        }]}

    parent_dag = {
        "_id": "parent_dag",
        "dag_params": {
            "dag_id": "parent_dag",
            "schedule": None,
            "catchup": False,
            "start_date": dtm.datetime.now(tz=dtm.timezone.utc)
        },
        "insert_timestamp": dtm.datetime.now(tz=dtm.timezone.utc),
        "task_params": [
            {"task_type": "python_operator",
             "python_callable_class_path": "integrator/test_trigger1.py",
             "class_name": "TestTrigger",
             "python_callable_function": "main",
             "params": {"task_id": "push_xcom"}
             },
            {"task_type": "trigger_multi_dag",
             "python_callable_class_path": "integrator/test_trigger2.py",
             "class_name": "TestTrigger",
             "python_callable_function": "main",
             "params": {"task_id": "generate_dag_run", "trigger_dag_id": "child_dag2"}
             }
        ]
    }

    parent_dag_workflow = {"_id": "parent_dag",
                           "workflow": {"push_xcom": "generate_dag_run"}}

    child_dag = {
        "_id": "child_dag2",
        "dag_params": {
            "dag_id": "child_dag2",
            "schedule": None,
            "catchup": False,
            "start_date": dtm.datetime.now(tz=dtm.timezone.utc)
        },
        "insert_timestamp": dtm.datetime.now(tz=dtm.timezone.utc),
        "task_params": [
            {"task_type": "python_operator",
             "python_callable_class_path": "integrator/clean_and_copy.py",
             "class_name": "CleanStagingBucketDataIntegrator",
             "python_callable_function": "main",
             "params": {"task_id": "child_dag_test"}
             }
        ]
    }

    child_dag_workflow = {"_id": "child_dag2",
                          "workflow": {}}

    pipeline_ids = {
        "_id": "pipeline_ids",
        "pipeline_list": [
            {"callback_test": 1}
        ]
    }

    # test_pipeline_workflow = {"_id": "test_pipeline",
    #                           "workflow": {"t1": "t2", "t2": "t3", "t3": "t4", "t4": "t5", "t5": "t6"}}

    # res1 = mongo_connector.replace_metadata(new_metadata=pipeline_ids, collection="airflow",
    #                                         document="pipeline_ids", pipeline_id="pipeline_ids")
    # res1 = mongo_connector.replace_metadata(new_metadata=pipeline_ids, collection="airflow",
    #                                         document="pipeline_ids", pipeline_id="pipeline_ids")

    mongo_connector.insert_metadata(data_to_insert=callback_test, collection="airflow",
                                    document="pipeline_config")
    # mongo_connector.insert_metadata(data_to_insert=callback_test_workflow, collection="airflow", document="pipeline_workflow")

    # a = mongo_connector.fetch_metadata(collection="airflow", document="global_args", pipeline_id="payer_20586")
    # print(a.get("test_jey", {"empty":"empty_val"}))

    # res1 = mongo_connector.insert_metadata(data_to_insert=callback_test, collection="airflow", document="pipeline_config")
    # res2 = mongo_connector.insert_metadata(data_to_insert=callback_test_workflow, collection="airflow", document="pipeline_workflow")

    # res1 = mongo_connector.replace_metadata(new_metadata=refresh_master_dev, collection="airflow", document="pipeline_config", pipeline_id="refresh_master_dev")
    # res2 = mongo_connector.replace_metadata(new_metadata=refresh_master_dev_workflow, collection="airflow", document="pipeline_workflow", pipeline_id="refresh_master_dev")

    # res1 = mongo_connector.replace_metadata(new_metadata=pipeline_ids, collection="airflow",
    #                                            document="pipeline_ids", pipeline_id="pipeline_ids")

    # res1 = mc.replace_metadata(new_metadata=auto_analytics_phase1, collection="airflow",
    #                                            document="pipeline_config", pipeline_id="auto_analytics_phase1")
    # res2 = mc.replace_metadata(new_metadata=auto_analytics_phase1_workflow, collection="airflow",
    #                                            document="pipeline_workflow",pipeline_id="auto_analytics_phase1")

    # res1 = mc.replace_metadata(new_metadata=child_dag_trigger_test, collection="airflow",
    #                                            document="pipeline_config", pipeline_id="child_dag_trigger_test")
    # res2 = mc.replace_metadata(new_metadata=child_dag_trigger_test_workflow, collection="airflow",
    #                                            document="pipeline_workflow",pipeline_id="child_dag_trigger_test")

    # print(res1)
    # print(res2)


def create_pipeline():
    # shell_commands = (
    #     'wget https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem'
    # )
    #
    # subprocess.check_call(shell_commands, shell=True)
    pipeline_id = "callback_test"

    dag_builder = AirflowDagBuilder()
    dag_obj: DAG = dag_builder.build_airflow_dag_object(pipeline_id=pipeline_id)
    # dag_obj
    # print(f'######### {dag_obj.dag_id} #############')
    print(f'######### {dag_obj.tree_view()} #############')
    print(dag_obj.default_args)
    # dag_obj.test(run_conf={
    #     "app": "ck-auto",
    #     "env": "dev",
    #     "partner_id": 20586,
    #     "retries": 0
    # })


if __name__ == "__main__":
    # shell_commands = (
    #     'if [ ! -f global-bundle.pem ]; then '
    #     'wget -P https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem; '
    #     'fi'
    # )
    #
    # subprocess.check_call(shell_commands, shell=True)
    url = "mongodb://sumit1:root@127.0.0.1:27017/"
    # url = "mongodb://sumit:sumit123@docdb-2024-06-26-08-32-24.cluster-cduks4mscct2.us-east-2.docdb.amazonaws.com:27017/?tls=true&tlsCAFile=global-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"
    mongo_connector = MongodbConnector(
        url=url
    )

    insert_to_mongo()
    # create_pipeline()
    # workflow=mongo_connector.fetch_metadata(collection="airflow",
    #                                      document="pipeline_workflow",
    #                                      pipeline_id="test_pipeline")["workflow"]
    # if isinstance(workflow, list):
    #     for current_workflow in workflow:
    #         print(current_workflow)
