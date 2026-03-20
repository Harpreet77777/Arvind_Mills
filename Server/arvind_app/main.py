from fastapi import Depends, FastAPI, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from . import crud, models, schemas
from .database import SessionLocal, engine

import logging
from .routers import (backend, planned_break, shift_data, operation_master, target)

log = logging.getLogger("uvicorn")
log.setLevel(logging.INFO)

models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="Ingenious Techzoid")

origins = [
    '*'
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


app.include_router(backend.router)
app.include_router(planned_break.router)
app.include_router(shift_data.router)
app.include_router(operation_master.router)
app.include_router(target.router)
