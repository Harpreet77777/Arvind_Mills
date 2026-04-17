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


router = APIRouter(tags=["Backend"])


@router.post("/start_po/")
async def start_po(po_data: schemas.RunPoBase, db: Session = Depends(get_db)):
    # check if prev po is running then show error
    prv_po = db.query(models.PoData).filter(models.PoData.machine_name == po_data.machine_name,
                                            models.PoData.stop_time.is_(None)).order_by(
        models.PoData.id.desc()).first()
    if prv_po:
        raise HTTPException(status_code=403, detail="Previous Po is running please stop or complete to start new PO")

    shift_data = await get_shift_details_data(db=db)
    date_ = await calculate_adjusted_date(shift_data["shift_a_start"],
                                          datetime.utcnow() + timedelta(hours=5, minutes=30))
    shift = await get_current_shift_data(db=db)

    db_po = models.PoData(**po_data.dict(),
                          date_=date_, shift=shift['shift'],
                          start_time=datetime.utcnow() + timedelta(hours=5, minutes=30), po_uuid=uuid.uuid4()
                          )
    db.add(db_po)
    db.commit()
    db.refresh(db_po)
    return db_po


@router.get("/{machine_name}")
async def get_current_po(machine_name: str, db: Session = Depends(get_db)):
    return db.query(models.PoData).filter(models.PoData.machine_name == machine_name,
                                          models.PoData.stop_time.is_(None)).order_by(
        models.PoData.id.desc()).first()


@router.post("/stop_po/{machine_name}/{is_partial_gr}")
async def stop_po(machine_name: str, is_partial_gr: bool = False, db: Session = Depends(get_db)):
    # check no po is running then show error
    current_po = db.query(models.PoData).filter(models.PoData.machine_name == machine_name,
                                                models.PoData.stop_time.is_(None)).order_by(
        models.PoData.id.desc()).first()
    if not current_po:
        raise HTTPException(status_code=403, detail="No Po is running on this machine")

    # update stop time and duration
    db_present_po = db.get(models.PoData, current_po.id)
    stop_time = datetime.utcnow() + timedelta(hours=5, minutes=30)
    start_time = db_present_po.start_time
    # if start_time.tzinfo is None:
    #     start_time = IST.localize(start_time)
    # else:
    #     start_time = start_time.astimezone(IST)
    duration = (stop_time - start_time).total_seconds()
    setattr(db_present_po, "stop_time", stop_time)
    setattr(db_present_po, "duration", duration)
    setattr(db_present_po, "is_complete", True)
    setattr(db_present_po, "is_partial_gr", is_partial_gr)
    db.add(db_present_po)
    db.commit()
    db.refresh(db_present_po)
    return db_present_po


# get and post api to fill real time data

def _to_ist(dt: datetime) -> datetime:
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        return IST.localize(dt)
    return dt.astimezone(IST)


def is_in_shift(start, end, now):
    """
    Check if current time falls within a shift.
    Handles both:
    - Normal shifts (e.g., 08:00 ? 16:00)
    - Cross-midnight shifts (e.g., 16:00 ? 00:00)
    """
    if start < end:
        return start <= now < end
    else:
        return now >= start or now < end


def _get_shift_for_time(db: Session, dt: datetime):
    # ? Get latest shift configuration
    current_shift_data = (
        db.query(models.ShiftMaster)
        .order_by(models.ShiftMaster.id.desc())
        .first()
    )

    if not current_shift_data:
        return "No_shift_data"


@router.post("/send_data/")
async def send_raw_data(raw_data: schemas.RawDataBase, db: Session = Depends(get_db)):
    raw_values = raw_data.normal_data or {}
    if not raw_values:
        raise HTTPException(status_code=400, detail="No raw data payload found")

    current_po = db.query(models.PoData).filter(models.PoData.machine_name == raw_data.machine_name,
                                                models.PoData.stop_time.is_(None)).order_by(
        models.PoData.id.desc()).first()
    if not current_po:
        raise HTTPException(status_code=403, detail="No Po is running on this machine")

    po_uuid = current_po.po_uuid
    dt_ist = raw_data.time_
    shift_data = await get_shift_details_data(db=db)
    row_date = await calculate_adjusted_date(shift_data["shift_a_start"],
                                             dt_ist)
    # row_date = dt_ist.date()
    row_hour = dt_ist.hour
    row_shift = _get_shift_for_time(db=db, dt=raw_data.time_)
    print(row_shift, row_hour, row_date, dt_ist)
    # No_shift_data 16 2026 - 04 - 09 2026 - 04 - 09 16: 59:51.889000 + 05: 30

    changed = []
    now = datetime.utcnow() + timedelta(hours=5, minutes=30)

    for key, value in raw_values.items():
        try:
            float_value = float(value)
        except (TypeError, ValueError):
            continue

        existing = db.query(models.HourlyData).filter(
            models.HourlyData.machine_name == raw_data.machine_name,
            models.HourlyData.po_uuid == po_uuid,
            models.HourlyData.date_ == row_date,
            models.HourlyData.shift == row_shift,
            models.HourlyData.hour == row_hour,
            models.HourlyData.key == key,
        ).first()

        if existing:
            if existing.key_start is None:
                existing.key_start = float_value

            existing.key_stop = float_value
            existing.difference_value = existing.key_stop - existing.key_start
            existing.updated_at = now
            db.add(existing)
            changed.append({"key": key, "action": "updated"})
        else:
            # For a new current-hour record, if prior consecutive hour exists on the same date,
            # carry previous hour's key_stop as key_start to avoid gaps in hourly aggregation.
            key_start_value = float_value
            if 0 < row_hour <= 23:
                prev_hour = row_hour - 1
                prev_entry = db.query(models.HourlyData).filter(
                    models.HourlyData.machine_name == raw_data.machine_name,
                    models.HourlyData.po_uuid == po_uuid,
                    models.HourlyData.date_ == row_date,
                    models.HourlyData.hour == prev_hour,
                    models.HourlyData.key == key,
                ).order_by(models.HourlyData.id.desc()).first()

                if prev_entry and prev_entry.key_stop is not None:
                    key_start_value = prev_entry.key_stop

            hourly = models.HourlyData(
                machine_name=raw_data.machine_name,
                section=current_po.section,
                line=current_po.line,
                date_=row_date,
                shift=row_shift,
                hour=row_hour,
                po_uuid=po_uuid,
                created_at=now,
                updated_at=None,
                key=key,
                key_start=key_start_value,
                key_stop=float_value,
                difference_value=float_value - key_start_value,
            )
            db.add(hourly)
            changed.append({"key": key, "action": "created"})

    db.commit()

    return {
        "status": "success",
        "machine_name": raw_data.machine_name,
        "po_uuid": po_uuid,
        "date": str(row_date),
        "shift": row_shift,
        "hour": row_hour,
        "processed_keys": len(changed),
        "details": changed,
    }


@router.get("/current_po_parameter/{machine_name}")
async def get_current_po_parameter(machine_name: str, db: Session = Depends(get_db)):
    current_po = db.query(models.PoData).filter(models.PoData.machine_name == machine_name,
                                                models.PoData.stop_time.is_(None)).order_by(
        models.PoData.id.desc()).first()
    if not current_po:
        raise HTTPException(status_code=403, detail="No Po is running on this machine")

    current_po_uuid = current_po.po_uuid

    # fetch hourly records for current PO
    hourly_rows = db.query(models.HourlyData).filter(
        models.HourlyData.po_uuid == current_po_uuid,
        models.HourlyData.machine_name == machine_name,
    ).order_by(models.HourlyData.date_.asc(), models.HourlyData.hour.asc(), models.HourlyData.id.asc()).all()

    if not hourly_rows:
        return {
            "machine_name": machine_name,
            "po_uuid": str(current_po_uuid),
            "status": "no_hourly_data",
            "data": [],
        }

    from collections import defaultdict

    key_grouped = defaultdict(list)
    for row in hourly_rows:
        key_grouped[row.key].append(row)

    result_data = []

    for key, rows in key_grouped.items():
        first = rows[0]
        last = rows[-1]

        # if only a single record exists
        if len(rows) == 1:
            first_key_start = first.key_start if first.key_start is not None else 0.0
            first_key_stop = first.key_stop if first.key_stop is not None else 0.0
            total_difference = first_key_stop - first_key_start
            result_data.append({
                "key": key,
                "first_hour": first.hour,
                "first_date": str(first.date_) if first.date_ is not None else None,
                "first_key_start": first.key_start,
                "first_key_stop": first.key_stop,
                "last_hour": first.hour,
                "last_date": str(first.date_) if first.date_ is not None else None,
                "last_key_start": first.key_start,
                "last_key_stop": first.key_stop,
                "difference": total_difference,
                "record_count": 1,
            })
            continue

        first_key_start = first.key_start if first.key_start is not None else 0.0
        last_key_stop = last.key_stop if last.key_stop is not None else 0.0
        total_difference = last_key_stop - first_key_start

        result_data.append({
            "key": key,
            "first_hour": first.hour,
            "first_date": str(first.date_) if first.date_ is not None else None,
            "first_key_start": first.key_start,
            "first_key_stop": first.key_stop,
            "last_hour": last.hour,
            "last_date": str(last.date_) if last.date_ is not None else None,
            "last_key_start": last.key_start,
            "last_key_stop": last.key_stop,
            "difference": total_difference,
            "record_count": len(rows),
        })

    return {
        "machine_name": machine_name,
        "po_uuid": str(current_po_uuid),
        "status": "success",
        "keys": len(result_data),
        "data": result_data,
    }


@router.get("/get_shift_by_time/")
async def get_shift_by_time(dt: datetime, db: Session = Depends(get_db)):
    return _get_shift_for_time(db=db, dt=dt)


@router.get("/get_po_details/{time_}")
async def get_po_according_to_time(time_: time, db: Session = Depends(get_db)):

    current_time = datetime.now(IST).time()

    if time_ > current_time:
        raise HTTPException(status_code=403, detail="Time should not be in the future")

    shift_data = await get_shift_details_data(db=db)

    date_ = await calculate_adjusted_date(shift_data["shift_a_start"],datetime.utcnow() + timedelta(hours=5, minutes=30))
    input_datetime = datetime.combine(date_, time_)

    po_details = db.query(models.PoData).filter(models.PoData.start_time <= input_datetime,
                                                or_(models.PoData.stop_time == None,
                                                    models.PoData.stop_time >= input_datetime)
                                                ).all()

    if not po_details:
        raise HTTPException(status_code=404, detail="No PO is running on this time")

    return po_details