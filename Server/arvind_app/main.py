from fastapi import Depends, FastAPI, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from fastapi.responses import FileResponse
from datetime import date, datetime
from . import crud, models, schemas, ms_database, Excel_Report
from . import ms_database as msdb
from .database import SessionLocal, engine
import aiofiles
from pathlib import Path
import logging
from .routers import (backend, finishing_report, dyeing_report, preparatory_report)

log = logging.getLogger("uvicorn")
log.setLevel(logging.INFO)
from . import Excel_Report

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
app.include_router(finishing_report.router)
app.include_router(dyeing_report.router)
app.include_router(preparatory_report.router)
