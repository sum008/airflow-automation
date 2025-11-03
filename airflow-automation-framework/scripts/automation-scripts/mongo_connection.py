from bytecodeairflow.pipeline_meta_fetcher.mongodb_connector import MongodbConnector
import datetime


def first_insert():
    mongo_connector = MongodbConnector()

    # poc_pipeline = {"_id": "poc_pipeline",
    #                 "dag_params": {"dag_id": "poc_pipeline",
    #                                "schedule": None,
    #                                "start_date": datetime.datetime.now(tz=datetime.timezone.utc)
    #                                },
    #                 "insert_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
    #                 "task_params": [
    #                     {"task_type": "python_operator",
    #                      "python_callable_path": "/home/sumit/Documents/dynamic-pipeline-framework-poc/poc_pipeline.py",
    #                      "python_callable_function": "fetch_query_results",
    #                      "callable_params": {"bucket_name": "jenkins-airflow-source-bucket",
    #                                          "secret_name": "snowflake/jenkins/poc/airflow",
    #                                          "file_path": "source_data/test.csv"},
    #                      "params": {"task_id": "fetch_query_results"}
    #                      },
    #                     {"task_type": "python_operator",
    #                      "python_callable_path": "/home/sumit/Documents/dynamic-pipeline-framework-poc/poc_pipeline.py",
    #                      "python_callable_function": "compare_and_sync_buckets",
    #                      "callable_params": {"source_path": "jenkins-airflow-source-bucket/source_data",
    #                                          "destination_path": "jenkins-airflow-destination-bucket/destination_folder"},
    #                      "params": {"task_id": "compare_and_sync_buckets_task"}
    #                      },
    #                     {"task_type": "emr_create_job_flow_operator",
    #                      "job_flow_overrides_path": "/home/sumit/Documents/dynamic-pipeline-framework-poc/emr_config.json",
    #                      "job_flow_overrides_key": "cluster_config",
    #                      "params": {"task_id": "create_emr_cluster", "aws_conn_id": "aws_default",
    #                                 "region_name": "us-east-2"
    #                                 }
    #                      },
    #                     {"task_type": "emr_add_steps_operator",
    #                      "steps_path": "/home/sumit/Documents/dynamic-pipeline-framework-poc/emr_config.json",
    #                      "steps_key": "spark_step",
    #                      "params": {"task_id": "add_spark_step", "aws_conn_id": "aws_default",
    #                                 "job_flow_id": "{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    #                                 "wait_for_completion": True
    #                                 }
    #                      },
    #                     {"task_type": "emr_step_sensor",
    #                      "params": {"task_id": "step_sensor",
    #                                 "job_flow_id": "{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    #                                 "step_id": "{{ task_instance.xcom_pull(task_ids='add_spark_step', key='return_value')[0] }}",
    #                                 "aws_conn_id": 'aws_default'
    #                                 }
    #                      },
    #                     {"task_type": "emr_terminate_job_flow_operator",
    #                      "params": {"task_id": "terminate_emr_cluster",
    #                                 "job_flow_id": "{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    #                                 "aws_conn_id": 'aws_default'
    #                                 }
    #                      },
    #                     {"task_type": "python_operator",
    #                      "python_callable_path": "/home/sumit/Documents/dynamic-pipeline-framework-poc/poc_pipeline.py",
    #                      "python_callable_function": "load_csv_into_snowflake",
    #                      "callable_params": {"bucket_name": "jenkins-airflow-destination-bucket",
    #                                          "secret_name": "snowflake/jenkins/poc/airflow",
    #                                          "file_path": "destination_folder/test.csv",
    #                                          "table_name": "test_airflow_poc"},
    #                      "params": {"task_id": "load_csv_into_snowflake"}
    #                      }
    #                 ]
    #                 }

    auto_analytics_phase1 = {"_id": "auto_analytics_phase1",
                             "dag_params": {"dag_id": "auto_analytics_phase1",
                                            "schedule": None,
                                            "start_date": datetime.datetime.now(tz=datetime.timezone.utc)
                                            },
                             "insert_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
                             "task_params": [
                                 {"task_type": "python_operator",
                                  "python_callable_path": "/usr/local/airflow/unifiedcloud/ck_pipelines/business_unit/auto_analytics/workflow_component/update_bucket_json.py",
                                  "python_callable_function": "get_payers_data",
                                  "callable_params": {
                                      "config_path": "/usr/local/airflow/unifiedcloud/ck_pipelines/business_unit/auto_analytics/configs/dev/config.json",
                                      "bucket_path": "/usr/local/airflow/unifiedcloud/ck_pipelines/business_unit/auto_analytics/configs/buckets.json"
                                  },
                                  "params": {"task_id": "update_bucket_json"}
                                  },
                                 {"task_type": "python_operator",
                                  "python_callable_path": "/usr/local/airflow/unifiedcloud/ck_pipelines/business_unit/auto_analytics/workflow_component/retrieve_filter_payers.py",
                                  "python_callable_function": "fetch_and_filter_payers",
                                  "callable_params": {
                                      "config_path": "/usr/local/airflow/unifiedcloud/ck_pipelines/business_unit/auto_analytics/configs/dev/config.json",
                                      "bucket_path": "/usr/local/airflow/unifiedcloud/ck_pipelines/business_unit/auto_analytics/configs/buckets.json"
                                  },
                                  "params": {"task_id": "fetch_and_filter_payers"}
                                  },
                                 {"task_type": "python_operator",
                                  "python_callable_path": "/usr/local/airflow/unifiedcloud/ck_pipelines/business_unit/auto_analytics/workflow_component/manifest_and_data_file.py",
                                  "python_callable_function": "filter_manifest_and_data_files",
                                  "callable_params": {
                                      "config_path": "/usr/local/airflow/unifiedcloud/ck_pipelines/business_unit/auto_analytics/configs/dev/config.json",
                                      "bucket_path": "/usr/local/airflow/unifiedcloud/ck_pipelines/business_unit/auto_analytics/configs/buckets.json"
                                  },
                                  "params": {"task_id": "filter_manifest_and_data_files"}
                                  },
                                 {"task_type": "python_operator",
                                  "python_callable_path": "/usr/local/airflow/unifiedcloud/ck_pipelines/business_unit/auto_analytics/workflow_component/data_validation.py",
                                  "python_callable_function": "validate_manifest_and_data_files",
                                  "callable_params": {
                                      "config_path": "/usr/local/airflow/unifiedcloud/ck_pipelines/business_unit/auto_analytics/configs/dev/config.json",
                                      "bucket_path": "/usr/local/airflow/unifiedcloud/ck_pipelines/business_unit/auto_analytics/configs/buckets.json"
                                  },
                                  "params": {"task_id": "validate_data_files"}
                                  },
                                 {"task_type": "python_operator",
                                  "python_callable_path": "/usr/local/airflow/unifiedcloud/ck_pipelines/business_unit/auto_analytics/workflow_component/clean_and_copy_data.py",
                                  "python_callable_function": "clean_data_files_from_stage_bucket",
                                  "callable_params": {
                                      "config_path": "/usr/local/airflow/unifiedcloud/ck_pipelines/business_unit/auto_analytics/configs/dev/config.json"
                                  },
                                  "params": {"task_id": "clean_data"}
                                  },
                                 {"task_type": "python_operator",
                                  "python_callable_path": "/usr/local/airflow/unifiedcloud/ck_pipelines/business_unit/auto_analytics/workflow_component/clean_and_copy_data.py",
                                  "python_callable_function": "copy_data_into_intermediate_bucket",
                                  "callable_params": {
                                      "config_path": "/usr/local/airflow/unifiedcloud/ck_pipelines/business_unit/auto_analytics/configs/dev/config.json"
                                  },
                                  "params": {"task_id": "copy_data"}
                                  }
                             ]
                             }

    auto_analytics_phase1_workflow = {"_id": "auto_analytics_phase1",
                                      "workflow": {"update_bucket_json": "fetch_and_filter_payers",
                                                   "fetch_and_filter_payers": "filter_manifest_and_data_files",
                                                   "filter_manifest_and_data_files": "validate_data_files",
                                                   "validate_data_files": "clean_data",
                                                   "clean_data": "copy_data"}}
    res1, res2 = "nothing", "nothing"
    try:
        res1 = mongo_connector.insert_metadata(data_to_insert=auto_analytics_phase1, collection="airflow",
                                               document="pipeline_config")
    except Exception as err:
        print(f"id already present... {err}")

    try:
        res2 = mongo_connector.insert_metadata(data_to_insert=auto_analytics_phase1_workflow, collection="airflow",
                                               document="pipeline_workflow")
    except Exception as err:
        print(f"id already present... {err}")

    return res1, res2


def test_connection():
    mongo_connector = MongodbConnector()
    doc = mongo_connector.fetch_metadata(pipeline_id="auto_analytics_phase1", collection="airflow",
                                         document="pipeline_config")
    print("PRINTING DOC....")
    print(doc)
