import json
import boto3
from typing import *


class Song(TypedDict):
    title: str
    artist: str
    year: int
    web_url: str
    img_url: str


def upload_music_entries(entries: list[Song]) -> NoReturn:
    db = boto3.resource('dynamodb')
    table = db.Table('Music')
    for song in entries:
        table.put_item(Item=song)


def read_music_file(file_name: str) -> list[Song]:
    with open(file_name) as json_file:
        music = json.load(json_file)
    return music["songs"]


def main() -> NoReturn:
    upload_music_entries(read_music_file('data/a1.json'))


if __name__ == '__main__':
    main()
