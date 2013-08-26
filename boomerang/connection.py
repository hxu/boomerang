"""
Connection wrapper for boto connections
"""

import boom_config
import boto


def connect_ec2(aws_access_key_id=None, aws_secret_access_key=None):
    if aws_access_key_id is None:
        aws_access_key_id = boom_config.AWS_ACCESS_KEY_ID
    if aws_secret_access_key is None:
        aws_secret_access_key = boom_config.AWS_SECRET_ACCESS_KEY
    return boto.connect_ec2(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)


def connect_s3(aws_access_key_id=None, aws_secret_access_key=None):
    if aws_access_key_id is None:
        aws_access_key_id = boom_config.AWS_ACCESS_KEY_ID
    if aws_secret_access_key is None:
        aws_secret_access_key = boom_config.AWS_SECRET_ACCESS_KEY
    return boto.connect_s3(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
