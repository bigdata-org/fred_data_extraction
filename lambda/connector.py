import snowflake.connector as sf
import os 
import boto3
def get_secret(secret_name):
    region_name = "us-east-1"
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e
    secret = get_secret_value_response['SecretString']
    return eval(secret) if secret is not None else None


def sf_connector(secret):
    # Create connection
    try:
        conn = sf.connect(
            user = secret['SF_USER'],
            password = secret['SF_PASSWORD'],
            account = secret['SF_ACCOUNT'],
            warehouse = secret['SF_WAREHOUSE'],
            database = secret['SF_DB'],
            role = secret['SF_ROLE'],
            private_key_file = 'rsa_key.p8'
        )
    except Exception as e:
        return None
    return conn