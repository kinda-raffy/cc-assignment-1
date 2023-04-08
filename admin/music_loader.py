from typing import NoReturn, TypedDict
import enum
import json
import boto3
from botocore.exceptions import WaiterError


class Song(TypedDict):
    """Represents a song in the database."""
    title: str
    artist: str
    year: int
    web_url: str
    img_url: str


def upload_music_entries(db_name: str, entries: list[Song]) -> NoReturn:
    """Uploads a list of songs to the database."""
    db = boto3.resource('dynamodb')
    table = db.Table(db_name)
    for song in entries:
        table.put_item(Item=song)
    print("Uploaded entries to DynamoDB.")


def read_music_file(file_name: str) -> list[Song]:
    """Reads a JSON file containing a list of songs."""
    with open(file_name) as json_file:
        music = json.load(json_file)
    return music["songs"]


class KeyType(enum.StrEnum):
    """Represents the type of key in a DynamoDB table."""
    PRIMARY = "HASH"
    SORT = "RANGE"


def create_table(
        table_name: str,
        *,
        partition: str | int,
        sort: str | int
) -> NoReturn:
    """Creates a DynamoDB table with the given name and partition/sort keys."""
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


def determine_s3_url(title: str) -> str:
    """Determines the S3 URL for the given song title."""
    bucket_name = "s3897093-rmit-song-images"
    region = "us-east-1"
    return f"https://{bucket_name}.s3.{region}.amazonaws.com/{title}"


def replace_img_urls(entries: list[Song]) -> list[Song]:
    """Replaces the img_url field with the S3 URL for the song."""
    for song in entries:
        song["img_url"] = determine_s3_url(song["title"])
    return entries


def main() -> NoReturn:
    db_name = "Music"
    create_table(db_name, partition="title", sort="artist")
    db_entries = replace_img_urls(read_music_file('data/a1.json'))
    upload_music_entries(db_name, db_entries)


if __name__ == "__main__":
    main()
