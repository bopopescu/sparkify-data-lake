import time
from datetime import datetime

import pandas as pd
import boto3
import json
import configparser


def get_config():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    return config


def create_emr_role(config):
    iam = iam_client(config)

    try:
        print('Creating a new IAM Role')
        iam.create_role(
            Path='/',
            RoleName=config.get("DWH", "DWH_IAM_EMR_ROLE_NAME"),
            Description='Default policy for the Amazon Elastic MapReduce service role.',
            AssumeRolePolicyDocument=json.dumps(
                {
                    'Statement': [{
                        'Action': 'sts:AssumeRole',
                        'Effect': 'Allow',
                        'Principal': {'Service': 'elasticmapreduce.amazonaws.com'}
                    }],
                    'Version': '2012-10-17'
                }
            )
        )

        print('Attaching Policy')

        attaching_policy_result = iam.attach_role_policy(
            RoleName=config.get("DWH", "DWH_IAM_EMR_ROLE_NAME"),
            PolicyArn="arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
        )['ResponseMetadata']['HTTPStatusCode']

        print(attaching_policy_result)

        print('Get the IAM role ARN')
        role_arn = iam_role(config.get("DWH", "DWH_IAM_EMR_ROLE_NAME"), iam)

        print(role_arn['Role']['Arn'])
    except Exception as e:
        print(e)


def create_ec2_role(config):
    iam = iam_client(config)

    try:
        print('Creating a new EC2 IAM Role')
        iam.create_role(
            Path='/',
            RoleName=config.get("DWH", "DWH_IAM_EC2_ROLE_NAME"),
            Description='Amazon EC2 service role...',
            AssumeRolePolicyDocument=json.dumps(
                {
                    'Statement': [{
                        'Sid': '',
                        'Action': 'sts:AssumeRole',
                        'Effect': 'Allow',
                        'Principal': {'Service': 'ec2.amazonaws.com'}
                    }],
                    'Version': '2008-10-17'
                }
            )
        )

        print('Attaching Policy')

        attaching_policy_result = iam.attach_role_policy(
            RoleName=config.get("DWH", "DWH_IAM_EC2_ROLE_NAME"),
            PolicyArn="arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
        )['ResponseMetadata']['HTTPStatusCode']

        print(attaching_policy_result)

        print('Get the IAM role ARN')
        role_arn = iam_role(config.get("DWH", "DWH_IAM_EC2_ROLE_NAME"), iam)

        print(role_arn['Role']['Arn'])

    except Exception as e:
        print(e)

    try:
        iam.create_instance_profile(
            InstanceProfileName=config.get('DWH', 'DWH_IAM_EC2_ROLE_NAME'),
            Path='/'
        )
        instance_profile = iam.get_instance_profile(
            InstanceProfileName=config.get('DWH', 'DWH_IAM_EC2_ROLE_NAME')
        )
        print(instance_profile)
        iam.add_role_to_instance_profile(
            InstanceProfileName=config.get('DWH', 'DWH_IAM_EC2_ROLE_NAME'),
            RoleName=config.get('DWH', 'DWH_IAM_EC2_ROLE_NAME')
        )
        print('-------')
        instance_profile = iam.get_instance_profile(
            InstanceProfileName=config.get('DWH', 'DWH_IAM_EC2_ROLE_NAME')
        )
        print(instance_profile)
    except Exception as e:
        print(e)


def iam_role(role_name, iam):
    return iam.get_role(
        RoleName=role_name,
    )


def iam_client(config):
    iam = boto3.client('iam',
                       region_name=config.get('DWH', 'REGION'),
                       aws_access_key_id=config.get('AWS', 'KEY'),
                       aws_secret_access_key=config.get('AWS', 'SECRET'))
    return iam


def emr_client(config):
    return boto3.client('emr',
                        region_name=config.get('DWH', 'REGION'),
                        aws_access_key_id=config.get('AWS', 'KEY'),
                        aws_secret_access_key=config.get('AWS', 'SECRET'))


def emr_cluster(config):
    emr = emr_client(config)
    try:
        response = emr.run_job_flow(
            Name=config.get("DWH", "DWH_CLUSTER_IDENTIFIER"), LogUri='s3://LOGS', ReleaseLabel='emr-5.29.0',
            Applications=[
                {
                    'Name': 'Spark'
                },
            ],
            Instances={
                'InstanceGroups': [
                    {
                        'Name': "Master",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': config.get("DWH", "DWH_NODE_TYPE"),
                        'InstanceCount': 1,
                    },
                    {
                        'Name': "Slave",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': config.get("DWH", "DWH_NODE_TYPE"),
                        'InstanceCount': int(config.get("DWH", "DWH_NUM_SLAVES")),
                    }
                ],
                'Ec2KeyName': 'spark-cluster',
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': 'subnet-3815fa53',
            },
            Steps=[],
            VisibleToAllUsers=True,
            JobFlowRole=config.get('DWH', 'DWH_IAM_EC2_ROLE_NAME'),
            ServiceRole=config.get('DWH', 'DWH_IAM_EMR_ROLE_NAME'),
            Tags=[
                {
                    'Key': 'tag_name_1',
                    'Value': 'tab_value_1',
                },
                {
                    'Key': 'tag_name_2',
                    'Value': 'tag_value_2',
                },
            ],
        )
        print(response['ResponseMetadata']['HTTPStatusCode'])
    except Exception as e:
        print(e)


def pretty_redshift_properties(props):
    pd.set_option('display.max_colwidth', -1)

    keys_to_show = ["Id", "Name", "Status", "MasterPublicDnsName"]
    x = [(k, v) for k, v in props['Cluster'].items() if k in keys_to_show]
    print(pd.DataFrame(data=x, columns=["Key", "Value"]))


def cluster_status():
    pretty_redshift_properties(cluster_properties())


def cluster_properties():
    config = get_config()
    emr = emr_client(config)

    return emr.describe_cluster(ClusterId=cluster_id(config, emr))


def cluster_id(config, emr):
    clusters = emr.list_clusters(
        CreatedAfter=datetime.today(),
    )['Clusters']

    return [cluster for cluster in clusters if cluster['Name'] == config.get("DWH", "DWH_CLUSTER_IDENTIFIER")][0]['Id']


def create_cluster():
    config = get_config()
    print('init')

    create_emr_role(config)
    create_ec2_role(config)

    time.sleep(20)
    print('ending')

    emr_cluster(config)
    cluster_status()


def delete_cluster():
    config = get_config()
    emr = emr_client(config)

    cluster = cluster_id(config, emr)
    print("Deleting EMR cluster " + str(cluster))

    emr.terminate_job_flows(
        JobFlowIds=[
            cluster,
        ]
    )

    # iam = iam_client(config)
    # print("Detaching policy...")
    # iam.detach_role_policy(RoleName=config.get("DWH", "DWH_IAM_EMR_ROLE_NAME"),
    #                        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    # print("Deleting role...")
    # iam.delete_role(RoleName=config.get("DWH", "DWH_IAM_EMR_ROLE_NAME"), )


def help():
    print("1. create_cluster()")
    print("2. cluster_status()")
    print("4. delete_cluster()")
