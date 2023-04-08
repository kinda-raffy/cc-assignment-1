from typing import NoReturn
import requests
import json
import boto3
from music_loader import (
    Song,
    read_music_file,
)


def create_image_bucket(bucket_name: str) -> NoReturn:
    """Creates an S3 bucket with the given name."""
    s3 = boto3.resource("s3")
    try:
        s3.create_bucket(Bucket=bucket_name)
    except s3.meta.client.exceptions.BucketAlreadyExists:
        print(f"Bucket {bucket_name} already exists.")


def make_bucket_public(bucket_name: str) -> NoReturn:
    """Makes the given S3 bucket public."""
    s3 = boto3.client("s3")
    s3.put_bucket_policy(
        Bucket=bucket_name,
        Policy=json.dumps({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "PublicRead",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": [
                        "s3:GetObject",
                        "s3:GetObjectVersion"
                    ],
                    "Resource": f"arn:aws:s3:::{bucket_name}/*"
                }
            ]
        })
    )


def upload_url_to_bucket(bucket_name: str, data: requests.Response, key: str) -> NoReturn:
    """Uploads the given data to the given S3 bucket with the given key."""
    s3 = boto3.resource("s3")
    s3.meta.client.upload_fileobj(
        Bucket=bucket_name,
        Key=key,
        Fileobj=data.raw,
    )


def stream_image(url: str) -> requests.Response:
    """Streams the image at the given URL."""
    return requests.get(url, stream=True)


def main() -> NoReturn:
    bucket_name = "s3897093-rmit-song-images"
    create_image_bucket(bucket_name)
    make_bucket_public(bucket_name)

    music_entries: list[Song] = read_music_file("data/a1.json")
    for entry in music_entries:
        upload_url_to_bucket(
            bucket_name,
            stream_image(entry["img_url"]),
            key=entry['title'],
        )


if __name__ == "__main__":
    main()
