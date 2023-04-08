from typing import Optional
from fastapi import (
    FastAPI,
    Header,
    HTTPException,
    APIRouter
)
import boto3
import pydantic
from mangum import Mangum


title = "Musify Account Service"
description = "This is a service for managing user accounts"
version = "0.1.0"
app = FastAPI(
    title=title,
    description=description,
    version=version,
)
handler = Mangum(app)
router = APIRouter(prefix="/accounts", tags=["accounts"])


@router.get("/__information__")
async def get_info():
    """Get the details of the service."""
    return {
        "name": title,
        "description": description,
        "version": version,
    }


class LoginData(pydantic.BaseModel):
    """Represents the data required to login a user."""
    email: str
    password: str


@router.post("/login")
async def login(req: LoginData):
    """Login an existing user."""
    if req.email is None or req.password is None:
        raise HTTPException(
            status_code=400,
            detail="Email or password is missing",
        )

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("Accounts")

    response = table.get_item(
        Key={
            "email": req.email,
        }
    )

    invalid_response = "Item" not in response or \
                       response["Item"]["password"] != req.password
    if invalid_response:
        raise HTTPException(
            status_code=404,
            detail="Email or password is invalid",
        )

    return response["Item"]


class RegisterData(pydantic.BaseModel):
    """Represents the data required to register a user."""
    email: str
    password: str
    username: str
    subscription: Optional[str] = list()


@router.post("/register")
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
    # Ensure the email does not already exist.
    account_exists = "Item" in response
    if account_exists:
        raise HTTPException(
            status_code=409,
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


@router.get("/subscriptions")
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
    # Get the user.
    response = table.get_item(
        Key={
            "email": email,
        }
    )
    # Ensure the user exists.
    invalid_response = "Item" not in response
    if invalid_response:
        raise HTTPException(
            status_code=404,
            detail="Email is invalid",
        )

    return response["Item"]["subscriptions"]


class SubscriptionData(pydantic.BaseModel):
    """Represents the data required to add or delete a subscription."""
    email: str
    song_title: str


@router.post("/subscriptions")
async def add_subscription(payload: SubscriptionData):
    """Add a subscription to a given user."""
    # Ensure the email and song title are not missing.
    if payload.email is None or payload.song_title is None:
        raise HTTPException(
            status_code=400,
            detail="Email or subscription is missing",
        )

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("Accounts")
    # Get the user.
    response = table.get_item(
        Key={
            "email": payload.email,
        }
    )
    # Ensure the user exists.
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


@router.delete("/subscriptions")
async def delete_subscription(payload: SubscriptionData):
    """Delete a subscription from a given user."""
    if payload.email is None or payload.song_title is None:
        raise HTTPException(
            status_code=400,
            detail="Email or subscription is missing",
        )

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("Accounts")
    # Get the user.
    response = table.get_item(
        Key={
            "email": payload.email,
        }
    )
    # Ensure the user exists.
    invalid_response = "Item" not in response
    if invalid_response:
        raise HTTPException(
            status_code=404,
            detail="Email is invalid",
        )
    # Find index for the song to delete.
    subscriptions = response["Item"]["subscriptions"]
    rm_song_index = subscriptions.index(payload.song_title)
    # Delete the subscription from the user.
    table.update_item(
        Key={
            "email": payload.email,
        },
        UpdateExpression=f"REMOVE subscriptions[{rm_song_index}]",
        ReturnValues="ALL_NEW"
    )


app.include_router(router)
