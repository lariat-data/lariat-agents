import boto3


def get_and_decrypt_keypair(
    aws_access_key_id: str,
    aws_secret_access_key: str,
    customer_account_id: str,
    role_arn_prefix: str,
    region_name: str,
):
    """
    aws_access_key_id: str - Access Key for sts_client to assume role
    aws_secret_access_key: str - Secret Access Key for sts client to assume role
    customer_account_id: str - Represents the customer account id that makes up the role_arn. Generally refers to an
    AWS customer account id or Azure Subscription ID
    role_arn_prefix: str - Prefix to the role_arn that has the customer account id appended to it
    region_name: str - Region Name for sts client
    :return: tuple of (aws_access_key_id, aws_secret_key, aws_session_token)
    """
    sts_client = boto3.client(
        "sts",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
    )

    role_arn = f"{role_arn_prefix}-{customer_account_id}"

    session_name = "s3-session-" + customer_account_id
    response = sts_client.assume_role(RoleArn=role_arn, RoleSessionName=session_name)

    # Extract the temp credentials from the assumed role
    temp_creds = response["Credentials"]
    return (
        temp_creds["AccessKeyId"],
        temp_creds["SecretAccessKey"],
        temp_creds["SessionToken"],
    )
