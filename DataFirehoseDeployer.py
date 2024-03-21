import json
from confluent_kafka.admin import AdminClient
import socket
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import sys
from re import match
import boto3
import logging
from botocore.exceptions import ClientError
import time

# Initialising Firehose and IAM clients
firehose_client = boto3.client('firehose')
iam_client = boto3.client('iam')


def oauth_cb(oauth_config):
    auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token("us-east-1")
    return auth_token, expiry_ms / 1000


def create_policy(msk_cluster_arn, s3_bucket):
    """
    :param msk_cluster_arn: Provide the MSK cluster ARN
    :param s3_bucket: The S3 bucket that Amazon Data Firehose will use to deliver records
    :return: This Function returns the IAM policy ARN.
    """

    # Here converting the provided Cluster ARN to topic and group ARNs, these ARNs will be used in the IAM policy.
    msk_topic_arn = msk_cluster_arn.replace("cluster", "topic", 1) + "/*"
    msk_group_arn = msk_cluster_arn.replace("cluster", "group", 1) + "/*"

    # Creating the Logs group ARN
    new = msk_cluster_arn.replace("kafka", "logs", 1)
    log_arn = new.replace(str(msk_cluster_arn.split(':')[-1]), "log-group:/aws/DataFirehose/*")

    # IAM policy which will be attached to the Role assumed by Data Firehose
    policy_doc = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "",
                "Effect": "Allow",
                "Action": [
                    "kafka:GetBootstrapBrokers",
                    "kafka:DescribeCluster",
                    "kafka:DescribeClusterV2",
                    "kafka-cluster:Connect"
                ],
                "Resource": msk_cluster_arn
            },
            {
                "Sid": "",
                "Effect": "Allow",
                "Action": [
                    "kafka-cluster:DescribeTopic",
                    "kafka-cluster:DescribeTopicDynamicConfiguration",
                    "kafka-cluster:ReadData"
                ],
                "Resource": msk_topic_arn
            },
            {
                "Sid": "",
                "Effect": "Allow",
                "Action": [
                    "kafka-cluster:DescribeGroup"
                ],
                "Resource": msk_group_arn
            },
            {
                "Sid": "",
                "Effect": "Allow",
                "Action": [
                    "s3:AbortMultipartUpload",
                    "s3:GetBucketLocation",
                    "s3:GetObject",
                    "s3:ListBucket",
                    "s3:ListBucketMultipartUploads",
                    "s3:PutObject"
                ],
                "Resource": [
                    "arn:aws:s3:::" + s3_bucket,
                    "arn:aws:s3:::" + s3_bucket + "/*"
                ]
            },
            {
                "Sid": "",
                "Effect": "Allow",
                "Action": [
                    "logs:PutLogEvents"
                ],
                "Resource": log_arn
            }
        ]
    }

    # Create IAM policy
    try:
        policy = iam_client.create_policy(
            PolicyName="MSK-to-DataFirehose",
            Description="This policy Enables Amazon Data Firehose to read from MSK",
            PolicyDocument=json.dumps(policy_doc),
        )
        logging.info("Created policy %s.", policy["Policy"]["Arn"])
    except ClientError:
        logging.exception("Couldn't create policy MSK-to-DataFirehose")
        raise
    else:
        return policy["Policy"]["Arn"]


# Attaching the IAM policy to the IAM Role
def attach_to_role(role_name, policy_arn):
    try:
        response = iam_client.attach_role_policy(
            PolicyArn=policy_arn,
            RoleName=role_name,
        )
        logging.debug(response)
        logging.info("Attached policy %s to role %s.", policy_arn, role_name)
    except ClientError:
        logging.exception("Couldn't attach policy %s to role %s.", policy_arn, role_name)
        raise


# Creating the IAM Role, this role will be assumed by Amazon Data Firehose.
def create_role(role_name):
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "firehose.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ]
    }
    try:
        role = iam_client.create_role(
            RoleName=role_name, AssumeRolePolicyDocument=json.dumps(trust_policy)
        )
        logging.info("Created role %s.", role["Role"]["RoleName"])
    except ClientError:
        logging.exception("Couldn't create role %s.", role_name)
        raise
    else:
        return role["Role"]["Arn"]


# Create Amazon Data Firehose for all the topics.
def create_delivery_streams(topic_list, role_arn, bucket, msk_cluster_arn, connectivity):

    """
    :param topic_list: List of topics to iterate
    :param role_arn: Role ARN to be assumed by Data Firehose
    :param bucket: S3 Bucket to which Firehose will deliver the data.
    :param msk_cluster_arn: MSK cluster ARN
    :param connectivity: Define Data Firehose Connectivity to MSK (Public/Private)
    """

    for topic_iter in topic_list:
        logging.info("Creating Firehose for topic : " + str(topic_iter))
        result = firehose_client.create_delivery_stream(
            # appending the Firehose name with topic name, creating prefix for each topic in the provided S3 bucket
            DeliveryStreamName='Firehose-' + str(topic_iter),
            DeliveryStreamType='MSKAsSource',
            ExtendedS3DestinationConfiguration={
                'RoleARN': role_arn,
                'BucketARN': 'arn:aws:s3:::' + bucket,
                'Prefix': 'MSK' + str(topic_iter) + '/',
                'ErrorOutputPrefix': '',
                'BufferingHints': {
                    'SizeInMBs': 1,
                    'IntervalInSeconds': 0
                },
                'CompressionFormat': 'UNCOMPRESSED',
                'CloudWatchLoggingOptions': {
                    'Enabled': True,
                    'LogGroupName': '/aws/DataFirehose/MSK' + str(topic_iter),
                    'LogStreamName': 'DestinationDelivery'
                }
            },
            MSKSourceConfiguration={
                'MSKClusterARN': msk_cluster_arn,
                'TopicName': topic_iter,
                'AuthenticationConfiguration': {
                    'RoleARN': role_arn,
                    'Connectivity': connectivity
                }
            }
        )

        logging.debug(result)
    logging.info("Completed")


def list_topics(pattern_in):
    """
    Get the list of topics from MSK and return the topics matching with Regex
    :param pattern_in: RegEx provided by the user
    :return: Returns the list of matching topics.
    """

    logging.info('Pattern provided :' + pattern_in)
    admin_client = AdminClient(config)

    admin_client.poll(10)

    topics_metadata = admin_client.list_topics(timeout=10)

    topic_names = [topic for topic in topics_metadata.topics]

    logging.info("List of all MSK Topics:" + str(topic_names))

    matched_topics = list(filter(lambda v: match(pattern_in, v), topic_names))
    logging.info("Qualified topics after regex matching :" + str(matched_topics))

    return matched_topics


if __name__ == '__main__':

    '''Usage : <bootstrap-brokers> <MSK Cluster ARN> <Cluster Connectivity (PUBLIC/PRIVATE)> <S3 Bucket Name> 
    <options: 1 for regex, 2 for topic list> <RegEx/topic list>'''

    logging.basicConfig(filename='Consumer.log', level=logging.INFO)
    logging.info('Started')

    broker = sys.argv[1]
    cluster_arn = sys.argv[2]
    connectivity = sys.argv[3]
    bucket = sys.argv[4]
    option = sys.argv[5]  # 1 for Regex, 2 for topic list
    topics_in = sys.argv[6:]

    # MSK Admin client configuration
    config = {
        # "debug": "all",
        'bootstrap.servers': broker,
        'client.id': socket.gethostname(),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'OAUTHBEARER',
        'oauth_cb': oauth_cb
    }

    # check if required number of arguments are passed while executing the script
    if len(sys.argv) < 6:
        sys.stderr.write(
            'Usage: %s <bootstrap-brokers> <MSK Cluster ARN> <Cluster Connectivity (Public/Private)> <S3 Bucket Name> '
            '<options: 1 for regex, 2 for topic list> <Regex/topic list>\n\n' %
            sys.argv[0])
        sys.exit(1)

    # Call Admin client to get the list of topics matching the RegEx.
    if option == "1":
        pattern = topics_in[0]
        topics = list_topics(pattern)

    # if list of topics provided, We do not validate if these topics exists in MSK.
    elif option == "2":
        topics = topics_in[0:]

    else:
        print("invalid option provided, select options: 1 for regex and 2 for topic list")
        sys.exit()

    logging.info("Topics Found/Matched: " + str(topics))

    # if matching topics found, proceed creating the IAM Role and Firehose Delivery streams
    if len(topics) > 0:
        inp = input(str(len(topics)) + " matching topics found, confirm creating " + str(
            len(topics)) + " delivery streams ? (Y to confirm) : ")
        if inp == ('y' or 'Y'):
            firehose_policy_arn = create_policy(cluster_arn, bucket)
            print(firehose_policy_arn)

            role_arn = create_role("MSK-to-DataFirehose-Role")
            print(role_arn)

            attach_to_role("MSK-to-DataFirehose-Role", firehose_policy_arn)
            # wait for the role to stabilise
            time.sleep(10)

            create_delivery_streams(topics, role_arn, bucket, cluster_arn, connectivity)
        else:
            print("Exiting, as not confirmed")
            sys.exit()
    else:
        logging.error('No matching Topics found, no delivery stream was deployed')