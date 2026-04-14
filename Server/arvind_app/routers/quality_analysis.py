from fastapi import Depends, APIRouter
from .. import schemas, models
from ..database import SessionLocal
from datetime import date, datetime, timedelta
from typing import List, Dict
from fastapi import HTTPException, status
from collections import defaultdict
import logging
import pytz
from sqlalchemy import func, case
from sqlalchemy.orm import Session
from ..routers.shift_data import get_current_shift_data, get_shift_details_data, calculate_adjusted_date

log_level = logging.INFO

FORMAT = '%(asctime)-15s %(levelname)-8s %(module)-15s:%(lineno)-8s %(message)s'

logFormatter = logging.Formatter(FORMAT)
log = logging.getLogger("Store")

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
log.addHandler(consoleHandler)
log.setLevel(log_level)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


router = APIRouter(tags=["Quality_Analysis"], prefix="/quality")

IST = pytz.timezone('Asia/Kolkata')


@router.get("/")
async def get_all_data(page: int = 1, size: int = 10, db: Session = Depends(get_db)):
    offset = (page - 1) * size
    return db.query(models.Quality).order_by(models.Quality.id.desc()).limit(size).offset(offset).all()


@router.post("/create")
async def create_quality(quality: schemas.QualityCreate, db: Session = Depends(get_db)):
    po = db.query(models.PoData).filter(models.PoData.machine_name == quality.machine_name,
                                        models.PoData.line == quality.line,
                                        models.PoData.po_number == quality.po_number).first()
    if not po:
        raise HTTPException(status_code=404, detail="Please upload the correct po_number")

    shift_data = await get_shift_details_data(db=db)
    date_ = await calculate_adjusted_date(shift_data["shift_a_start"], datetime.now(IST))
    shift = await get_current_shift_data(db=db)
    new_quality = models.Quality(date_=date_, shift=shift["shift"], **quality.dict())
    db.add(new_quality)
    db.commit()
    db.refresh(new_quality)
    return new_quality

@router.get("/get_ng_data")
async def get_ng_data(date_: date,shift: schemas.ShiftEnum,machine_name: str,line: str,
    db: Session = Depends(get_db)):
    query = db.query(func.sum(models.Quality.value)).filter(models.Quality.date_ == date_,
                                                            models.Quality.machine_name == machine_name,
                                                            models.Quality.line == line,
                                                            models.Quality.key == "Length")
    if shift != schemas.ShiftEnum.ALL_SHIFT:
        query = query.filter(models.Quality.shift == shift)

    total_length = query.scalar()

    return {"not_ok": total_length or 0}


@router.get("/calculate_quantity")
async def calculate_quantity(date_: date,shift: schemas.ShiftEnum,machine_name: str,line: str,
    db: Session = Depends(get_db)):

    ng_data = await get_ng_data(date_, shift, machine_name, line, db)
    query = db.query(models.HourlyData).filter(models.HourlyData.machine_name == machine_name,
                                               models.HourlyData.line == line,
                                               models.HourlyData.date_ == date_,
                                               models.HourlyData.key == "Length")

    if shift != schemas.ShiftEnum.ALL_SHIFT:
        query = query.filter(models.HourlyData.shift == shift)

    hourly_data = query.order_by(models.HourlyData.created_at.asc()).all()

    if not hourly_data:
        return {"ok": 0, "not_ok": ng_data["not_ok"]}

    first_value = hourly_data[0].key_start
    last_value = hourly_data[-1].key_stop
    difference = max(0, last_value - first_value)
    return {
        "ok": difference,
        "not_ok": ng_data["not_ok"]
    }
