from typing import Optional
from fastapi import FastAPI, Query, HTTPException
import boto3


app = FastAPI()


@app.get("/all")
async def get_all_music():
    db = boto3.resource("dynamodb")
    table = db.Table("Music")
    response = table.scan()
    return response["Items"]


@app.get("/")
async def get_music(
        title: Optional[str] = Query(None, min_length=1, max_length=100),
        artist: Optional[str] = Query(None, min_length=1, max_length=100),
        year: Optional[int] = Query(None)
):
    if not any([title, artist, year]):
        raise HTTPException(
            status_code=400,
            detail="No query parameters provided"
        )

    db = boto3.client("dynamodb")
    condition = list()
    attribute_names = dict()
    attribute_values = dict()
    if title:
        condition.append("title = :title")
        attribute_values[":title"] = {"S": title}
    if artist:
        condition.append("artist = :artist")
        attribute_values[":artist"] = {"S": artist}
    if year:
        condition.append("#yr = :year")
        attribute_names["#yr"] = "year"
        attribute_values[":year"] = {"N": str(year)}

    print("CONDITION ", condition)
    print("ATTR ", attribute_values)
    response = db.scan(
        TableName="Music",
        FilterExpression=" AND ".join(condition),
        ExpressionAttributeNames=attribute_names,
        ExpressionAttributeValues=attribute_values
    )
    return response["Items"]
