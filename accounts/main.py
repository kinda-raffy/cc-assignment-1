from typing import Optional
from fastapi import (
    FastAPI,
    Header,
    HTTPException,
)
import boto3
import pydantic


app = FastAPI(
    title="Musify Account Service",
    description="This is a service for managing user accounts",
    version="0.1.0",
)


@app.get("/")
async def login(
        email: str = Header(None, description="Email of the user"),
        password: str = Header(None, description="Password of the user"),
):
    """Login an existing user."""
    if email is None or password is None:
        raise HTTPException(
            status_code=400,
            detail="Email or password is missing",
        )

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("Accounts")

    response = table.get_item(
        Key={
            "email": email,
        }
    )

    invalid_response = "Item" not in response or \
                       response["Item"]["password"] != password
    if invalid_response:
        raise HTTPException(
            status_code=404,
            detail="Email or password is invalid",
        )

    return response["Item"]


class RegisterData(pydantic.BaseModel):
    email: str
    password: str
    username: str
    subscription: Optional[str] = list()


@app.post("/")
async def register(user: RegisterData):
    """Register a new user."""
    missing_required_fields: bool = None in (
        user.email,
        user.password,
        user.username
    )
    if missing_required_fields:
        raise HTTPException(
            status_code=400,
            detail="Email, password or username is missing",
        )

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("Accounts")

    response = table.get_item(
        Key={
            "email": user.email,
        }
    )
    account_exists = "Item" in response
    if account_exists:
        raise HTTPException(
            status_code=400,
            detail="The email already exists",
        )

    # Create a new account.
    table.put_item(
        Item={
            "email": user.email,
            "password": user.password,
            "username": user.username,
            "subscriptions": user.subscription,
        }
    )

    return {
        "email": user.email,
        "username": user.username,
    }


@app.get("/subscriptions")
async def get_subscriptions(
        email: str = Header(None, description="Email of the user"),
):
    """Get the subscriptions of a user."""
    if email is None:
        raise HTTPException(
            status_code=400,
            detail="Email is missing",
        )

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("Accounts")

    response = table.get_item(
        Key={
            "email": email,
        }
    )

    invalid_response = "Item" not in response
    if invalid_response:
        raise HTTPException(
            status_code=404,
            detail="Email is invalid",
        )

    return response["Item"]["subscriptions"]


class SubscriptionData(pydantic.BaseModel):
    email: str
    song_title: str


@app.post("/subscriptions")
async def add_subscription(payload: SubscriptionData):
    """Add a subscription to a user."""
    if payload.email is None or payload.song_title is None:
        raise HTTPException(
            status_code=400,
            detail="Email or subscription is missing",
        )

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("Accounts")

    response = table.get_item(
        Key={
            "email": payload.email,
        }
    )

    invalid_response = "Item" not in response
    if invalid_response:
        raise HTTPException(
            status_code=404,
            detail="Email is invalid",
        )

    # Add the subscription to the user.
    table.update_item(
        Key={
            "email": payload.email,
        },
        UpdateExpression="SET subscriptions = list_append(subscriptions, :i)",
        ExpressionAttributeValues={
            ":i": [payload.song_title],
        },
        ReturnValues="UPDATED_NEW"
    )

    return {
        "email":         payload.email,
        "subscriptions": response["Item"]["subscriptions"] + [payload.song_title],
    }


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
