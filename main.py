from typing import NoReturn
import enum
import boto3
from mypy_boto3_dynamodb.service_resource import *


class KeyType(enum.StrEnum):
    PRIMARY = "HASH"
    SORT = "RANGE"


def create_table(
        table_name: str,
        *,
        partition: str | int,
        sort: str | int
) -> Table:
    db = boto3.resource("dynamodb")
    partition_type = "S" if isinstance(partition, str) else "N"
    sort_type = "S" if isinstance(sort, str) else "N"

    return db.create_table(
        TableName=table_name,
        KeySchema=[
            {
                "AttributeName": partition,
                "KeyType":       KeyType.PRIMARY,
            },
            {
                "AttributeName": sort,
                "KeyType":       KeyType.SORT,
            }
        ],
        AttributeDefinitions=[
            {
                "AttributeName": partition,
                "AttributeType": partition_type,
            },
            {
                "AttributeName": sort,
                "AttributeType": sort_type,
            },
        ],
        ProvisionedThroughput={
            "ReadCapacityUnits":  10,
            "WriteCapacityUnits": 10
        }
    )


def main() -> NoReturn:
    create_table("Music", partition="title", sort="artist")


if __name__ == "__main__":
    main()
