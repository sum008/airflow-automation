from botocore.exceptions import ClientError
import boto3
import json
import logging

logger= logging.getLogger(__name__)

def load_credentials_aws(secret_name: str, region_name: str) -> dict:
    """
        provide credentials detail from secret manager
        Args :
            secret_name(str) - unique name for your secret value
            region_name(str) - region where your secret is stored
        Returns :
            Dictionary containing secret data
    """
    logger.info(f"Inside load_credentials {secret_name=} and {region_name=}")
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as error:
        raise error

    secret_string = get_secret_value_response['SecretString']
    secret = json.loads(secret_string)
    return secret
