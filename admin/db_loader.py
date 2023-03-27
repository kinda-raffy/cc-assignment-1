from typing import NoReturn, TypedDict
import enum
import json
import boto3
from mypy_boto3_dynamodb.service_resource import *


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
    db_name = "Music"
    create_table(db_name, partition="title", sort="artist")
    upload_music_entries(db_name, read_music_file('data/a1.json'))


if __name__ == "__main__":
    main()
