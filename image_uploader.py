from typing import *

from requests import Response

from loader import (
    Song,
    read_music_file,
)
import boto3
import requests


def create_image_bucket(bucket_name: str) -> NoReturn:
    s3 = boto3.resource("s3")
    try:
        s3.create_bucket(Bucket=bucket_name)
    except s3.meta.client.exceptions.BucketAlreadyExists:
        print(f"Bucket {bucket_name} already exists.")


def upload_url_to_bucket(bucket_name: str, data: Response, key: str) -> NoReturn:
    s3 = boto3.resource("s3")
    s3.meta.client.upload_fileobj(
        Bucket=bucket_name,
        Key=key,
        Fileobj=data.raw,
    )


def download_image(url: str) -> Response:
    return requests.get(url, stream=True)


def main() -> NoReturn:
    bucket_name = "test-music-images"
    create_image_bucket(bucket_name)

    music_entries: list[Song] = read_music_file("data/a1.json")
    for entry in music_entries:
        upload_url_to_bucket(
            bucket_name,
            download_image(entry["img_url"]),
            key=f"{entry['title']}-{entry['artist']}",
        )


if __name__ == "__main__":
    main()
