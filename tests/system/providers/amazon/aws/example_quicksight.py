# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from datetime import datetime

import boto3

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.operators.quicksight import QuickSightCreateIngestionOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3CreateObjectOperator
from airflow.providers.amazon.aws.sensors.quicksight import QuickSightSensor
from tests.system.providers.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

"""
Prerequisites:
1. The account which runs this test must manually be activated in Quicksight here:
https://quicksight.aws.amazon.com/sn/console/signup?#
2. The activation process creates an IAM Role called `aws-quicksight-service-role-v0`.
 You have to add a policy named 'AWSQuickSightS3Policy' with the S3 access permissions.
 The policy name is enforced, the permissions json can be copied from `AmazonS3FullAccess`.
"""

DAG_ID = 'example_quicksight'

sys_test_context_task = SystemTestContextBuilder().build()

SAMPLE_DATA_COLUMNS = {'Project': 'STRING', 'Year': 'INTEGER'}
SAMPLE_DATA = """'Airflow',2015
    'OpenOffice',2012
    'Subversion',2000
    'NiFi',2006
"""


@task
def get_aws_account_id() -> int:
    return boto3.client('sts').get_caller_identity()['Account']


@task
def await_bucket(bucket: str):
    boto3.client('s3').get_waiter('bucket_exists').wait(Bucket=bucket)


@task
def create_quicksight_dataset(aws_account_id: int, dataset_name: str, bucket: str):
    table_map = {
        'default': {
            'S3Source': {
                'DataSourceArn': f'arn:aws:s3:::{bucket}',
                'InputColumns': [
                    {'Name': _name, 'Type': _type} for (_name, _type) in SAMPLE_DATA_COLUMNS.items()
                ],
            }
        }
    }

    return boto3.client('quicksight').create_data_set(
        AwsAccountId=aws_account_id,
        DataSetId=dataset_name,
        Name=dataset_name,
        PhysicalTableMap=table_map,
        ImportMode='SPICE',
    )['DataSetId']


with DAG(
    dag_id=DAG_ID,
    schedule_interval='@once',
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    account_id = get_aws_account_id()

    env_id = test_context[ENV_ID_KEY]
    bucket_name = f'{env_id}-quicksight-bucket'
    dataset_id = f'{env_id}-dataset'
    ingestion_id = f'{env_id}-ingestion'

    create_s3_bucket = S3CreateBucketOperator(task_id='create_s3_bucket', bucket_name=bucket_name)
    await_create_bucket = await_bucket(bucket_name)

    upload_sample_data = S3CreateObjectOperator(
        task_id='upload_sample_data',
        s3_bucket=bucket_name,
        s3_key='sample_data.csv',
        data=SAMPLE_DATA,
        replace=True,
    )

    create_dataset = create_quicksight_dataset(account_id, dataset_id, bucket_name)

    # [START howto_operator_quicksight_create_ingestion]
    create_ingestion = QuickSightCreateIngestionOperator(
        task_id='create_ingestion',
        data_set_id=create_dataset,
        ingestion_id=ingestion_id,
        # Waits by default, setting as False to demonstrate the Sensor below.
        wait_for_completion=False,
    )
    # [END howto_operator_quicksight_create_ingestion]

    # [START howto_sensor_quicksight]
    await_job = QuickSightSensor(
        task_id='await_job',
        data_set_id=create_dataset,
        ingestion_id=ingestion_id,
    )
    # [END howto_sensor_quicksight]

    chain(
        # TEST SETUP
        test_context,
        account_id,
        create_s3_bucket,
        await_create_bucket,
        upload_sample_data,
        create_dataset,
        # TEST BODY
        create_ingestion,
        await_job,
        # TEST TEARDOWN
        # delete_ingestion(ingestion_id),
        # delete_dataset(dataset_id),
        # delete_bucket(bucket_name),
    )
