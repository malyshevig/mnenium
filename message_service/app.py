from fastapi import FastAPI, Depends, HTTPException, Query
from flask import jsonify
from sqlalchemy.orm import Session
from typing import List, Optional
from crud import Persist
import uvicorn
from common.model import Message
import schemas
import logging
from fastapi import FastAPI, HTTPException
from pydantic import ValidationError
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from message_service.schemas import MessageUpdate

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)



crud = Persist()
app = FastAPI(title="Message Service API")

from fastapi import FastAPI, Request, Response
import logging

# Пример простого middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    logging.info(f"Пришел запрос: {request.method} {request.url}")
    response = await call_next(request)
    logging.info(f"Ответ: {response.status_code}")
    return response

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
    return JSONResponse(
        status_code=422,
        content={
            "detail": exc.errors(),
            "body": exc.body
        },
    )


@app.post("/message/")
def create_message(message: schemas.Message):
    """Создать новое сообщение"""
    print (message)
    id = crud.create_message(message=message)
    return {"id": id}

@app.get("/message/{message_id}", response_model=Message)
def read_message(message_id: int):
    """Получить сообщение по ID"""
    db_message = crud.get_messages( message_id=message_id)
    if db_message is None:
        raise HTTPException(status_code=404, detail="Message not found")
    return db_message


@app.put("/message/{message_id}")
def update_message(
    message_id: int,
    message_update: MessageUpdate):
    logging.info(f"Обновление сообщения {message_id} на {message_update.class_id}")
    db_message = crud.update_message_class(message_id=message_id, class_id=message_update.class_id, text=message_update.text)
    if db_message is None:
        raise HTTPException(status_code=404, detail="Message not found")
    return {}


@app.get("/message/", response_model=List[schemas.Message])
def read_message():
    """Получить сообщение по ID"""
    db_messages = crud.get_messages()
    if db_messages is None:
        raise HTTPException(status_code=404, detail="Message not found")
    return db_messages


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0",  port=8003, log_level="info")