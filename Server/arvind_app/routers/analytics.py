from fastapi import Depends, FastAPI, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from fastapi.responses import FileResponse
from sqlalchemy import cast, Time, or_, and_
from datetime import date, datetime, timedelta, time
from ..routers.shift_data import get_current_shift_data, get_shift_details_data, calculate_adjusted_date
from .. import crud, models, schemas
from ..database import SessionLocal, engine
import uuid
import pytz
from pathlib import Path
from fastapi import Depends, APIRouter
import logging

log = logging.getLogger("uvicorn")
log.setLevel(logging.INFO)

IST = pytz.timezone('Asia/Kolkata')


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


router = APIRouter(tags=["Analytics"])

@router.get("/get_po_data/{from_date}/{to_date}")
async def get_po_data(from_date: date, to_date: date,db:Session = Depends(get_db)):
    po_data = db.query(models.PoData).filter(models.PoData.date_.between(from_date,to_date),
                                             models.PoData.stop_time != None).all()
    return [{"po_number": data.po_number,
            "machine_name": data.machine_name,
            "po_uuid": data.po_uuid,
            "start_time": data.start_time,
            "stop_time": data.stop_time
             }for data in po_data]



async def calculate_po_quantity(po_uuid: uuid, db: Session):
    hourly_data = db.query(models.HourlyData).filter(models.HourlyData.po_uuid == po_uuid,
                                                     models.HourlyData.key == "Length").order_by(
        models.HourlyData.id.desc()).first()

    last_value = hourly_data.key_stop
    return {
        "last_po_length": last_value,
    }


@router.get("/po_details/{po_uuid}")
async def get_po_details(po_uuid: str, db: Session = Depends(get_db)):
    po_details = db.query(models.PoData).filter(models.PoData.po_uuid == po_uuid,
                                                models.PoData.stop_time != None).first()
    calculated_length = await calculate_po_quantity(po_uuid=po_details.po_uuid, db=db)
    return {**po_details.__dict__,
            "last_po_length": calculated_length["last_po_length"]}
