from typing import Optional
from fastapi import FastAPI, Query, HTTPException
import boto3
import pydantic


app = FastAPI()


@app.get("/all")
async def get_all_music():
    db = boto3.resource("dynamodb")
    table = db.Table("Music")
    response = table.scan()
    return response["Items"]


@app.get("/")
async def get_music(
        title: Optional[str] = Query(None),
        artist: Optional[str] = Query(None),
        year: Optional[int] = Query(None)
) -> list[dict]:
    no_params_provided = not any([title, artist, year])
    if no_params_provided:
        raise HTTPException(
            status_code=400,
            detail="No query parameters provided"
        )

    db = boto3.client("dynamodb")
    condition = list()
    attribute_name = dict()
    attribute_values = dict()
    if title:
        condition.append(f"title = :_title")
        attribute_values[":_title"] = {"S": title}
    if artist:
        condition.append(f"artist = :_artist")
        attribute_values[":_artist"] = {"S": artist}
    if year:
        condition.append(f"#yr = :_year")
        attribute_name["#yr"] = "year"
        attribute_values[":_year"] = {"S": str(year)}

    condition = " AND ".join(condition)
    attribute_name_exist = attribute_name if attribute_name else None
    if attribute_name_exist:
        response = db.scan(
            TableName="Music",
            FilterExpression=condition,
            ExpressionAttributeNames=attribute_name,
            ExpressionAttributeValues=attribute_values
        )
    else:
        response = db.scan(
            TableName="Music",
            FilterExpression=condition,
            ExpressionAttributeValues=attribute_values
        )
    return flatten_response_dict(response["Items"])


def flatten_response_dict(response: list[dict]) -> list[dict]:
    return [
        {
            key: value["S"]
            for key, value in item.items()
        }
        for item in response
    ]
