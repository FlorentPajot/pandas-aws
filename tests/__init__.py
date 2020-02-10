__author__ = 'fpajot'

from unittest import TestCase

import mock

from pandas_aws import get_client


class GetAWSClientTests(TestCase):
    """Test related AWS client module functions."""

    @mock.patch('pandas_aws.boto3')
    def test_get_aws_client_success(self, mock_boto):
        """Test AWS client"""

        _ = get_client('s3')
        mock_boto.Session.assert_called_with(profile_name='default')

    @mock.patch('pandas_aws.boto3')
    def test_get_aws_client_custom_profile(self, mock_boto):
        """Test AWS client with wrong profile"""

        _ = get_client(service_name='s3', profile_name='custom_profile')
        mock_boto.Session.assert_called_with(profile_name='custom_profile')

    @mock.patch('pandas_aws.boto3')
    def test_get_aws_client_specific_service(self, mock_boto):
        """Test AWS client"""

        client = get_client('sns')
        self.assertIs(client, mock_boto.Session().client(service_name='sns'))
