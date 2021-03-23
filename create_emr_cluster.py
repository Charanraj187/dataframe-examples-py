import boto3


def lambda_handler(event, context):

    client = boto3.client('emr',  region_name="eu-west-1")

    instances = {
        'MasterInstanceType': 'm5.xlarge',
        'SlaveInstanceType': 'm5.xlarge',
        'InstanceCount': 2,
        'InstanceGroups': [],
        'Ec2KeyName': 'charkey',
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
        'Ec2SubnetId': 'subnet-0ecc9d54',
        'EmrManagedMasterSecurityGroup': 'sg-095e3223bf548c1b7',
        'EmrManagedSlaveSecurityGroup': 'sg-0b890a42986c04d7f'
    }

    configurations = [
        {
            'Classification': 'yarn-site',
            'Properties': {
                'yarn.resourcemanager.scheduler.class': 'org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler'
            },
            'Configurations': []
        },
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {
                        "PYSPARK_PYTHON": "/usr/bin/python3"
                    }
                }
            ]
        }
    ]

    response = client.run_job_flow(
        Name='Charan_L_Cluster',
        LogUri='s3://charan-1234/logs',
        ReleaseLabel='emr-5.30.0',
        Instances=instances,
        Configurations=configurations,
        Steps=[],
        BootstrapActions=[],
        Applications=[
            {'Name': 'Spark'},
            {'Name': 'Zeppelin'},
            {'Name': 'Ganglia'}
        ],
        VisibleToAllUsers=True,
        ServiceRole='EMR_DefaultRole',
        JobFlowRole='EMR_EC2_DefaultRole',
        AutoScalingRole='EMR_AutoScaling_DefaultRole',
        EbsRootVolumeSize=30
    )
    return response["JobFlowId"]
