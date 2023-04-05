from typing import NoReturn, TypedDict
import enum
import json
import boto3
from botocore.exceptions import WaiterError


class Song(TypedDict):
    title: str
    artist: str
    year: int
    web_url: str
    img_url: str


def upload_music_entries(db_name: str, entries: list[Song]) -> NoReturn:
    db = boto3.resource('dynamodb')
    table = db.Table(db_name)
    for song in entries:
        table.put_item(Item=song)
    print("Uploaded entries to DynamoDB.")


def read_music_file(file_name: str) -> list[Song]:
    with open(file_name) as json_file:
        music = json.load(json_file)
    return music["songs"]


class KeyType(enum.StrEnum):
    PRIMARY = "HASH"
    SORT = "RANGE"


def create_table(
        table_name: str,
        *,
        partition: str | int,
        sort: str | int
) -> NoReturn:
    db = boto3.resource("dynamodb")
    partition_type = "S" if isinstance(partition, str) else "N"
    sort_type = "S" if isinstance(sort, str) else "N"

    db.create_table(
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

    try:
        db.meta.client.get_waiter("table_exists").wait(TableName=table_name)
        print("Table created successfully.")
    except WaiterError:
        print("Table did not become active within the specified timeout period.")


def main() -> NoReturn:
    db_name = "Music"
    create_table(db_name, partition="title", sort="artist")
    upload_music_entries(db_name, read_music_file('data/a1.json'))


if __name__ == "__main__":
    main()
