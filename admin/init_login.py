import boto3


def initialise_accounts_table(dynamodb):
    """Initialise the Accounts table."""
    table = dynamodb.create_table(
        TableName="Accounts",
        KeySchema=[
            {
                "AttributeName": "email",
                "KeyType": "HASH",
            },
        ],
        AttributeDefinitions=[
            {
                "AttributeName": "email",
                "AttributeType": "S",
            },
        ],
        ProvisionedThroughput={
            "ReadCapacityUnits": 5,
            "WriteCapacityUnits": 5,
        },
    )

    table.meta.client.get_waiter("table_exists").wait(TableName="Accounts")
    return dynamodb


if __name__ == "__main__":
    dynamodb = boto3.resource("dynamodb")
    try:
        initialise_accounts_table(dynamodb)
    except dynamodb.meta.client.exceptions.ResourceInUseException:
        pass
