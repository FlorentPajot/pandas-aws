__author__ = 'fpajot'

from botocore.exceptions import ProfileNotFound
import boto3


def get_client(service_name: str, profile_name: str = 'default'):
    """Get AWS client for a specific service while handling credentials via profile"""
    try:
        session = boto3.Session(profile_name=profile_name)
        return session.client(service_name)
    except ProfileNotFound:
        return boto3.client(service_name=service_name)


__all__ = ['s3']
