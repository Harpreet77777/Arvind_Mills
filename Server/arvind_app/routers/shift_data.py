from datetime import timedelta, datetime, time, date

import sqlalchemy
from sqlalchemy import func
import pytz
from fastapi import Depends, APIRouter, HTTPException
from sqlalchemy import or_
from sqlalchemy.orm import Session
from typing import List, Dict, Any
from .. import schemas, crud, models
from ..database import SessionLocal
from ..schemas import ShiftEnum
import logging
from fastapi import HTTPException, status

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


router = APIRouter(tags=["Dynamic Shift Data"], prefix="/dynamic_shift_data")


#### ROUTERS URLS HERE
@router.post("/create_shift_data/")
async def create_shift_data(shift_data: schemas.ShiftMasterBase, db: Session = Depends(get_db)):
    return await create_shift_data_new(db=db, shift_data=shift_data)


@router.get("/get_shift_details_data/")
async def get_shift_details_data(db: Session = Depends(get_db)):
    return await get_shift_details(db=db)



#@router.get("/get_current_shift_data_old/")
async def get_current_shift_data_old(db: Session = Depends(get_db)):
    current_shift_data = db.query(models.ShiftMaster).order_by(models.ShiftMaster.id.desc()).first()

    if not current_shift_data:
        return {
            "shift": "No_shift_data",
            "time_": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        }

    # Convert UTC?IST
    now = (datetime.utcnow() + timedelta(hours=5, minutes=30)).time()

    shift = None

    # ---- SHIFT A ----
    if current_shift_data.shift_a_start and current_shift_data.shift_a_end:
        a_start = current_shift_data.shift_a_start.time()
        a_end = current_shift_data.shift_a_end.time()
        if a_start <= now < a_end:
            shift = "A"

    # ---- SHIFT B ----
    if not shift and current_shift_data.shift_b_start and current_shift_data.shift_b_end:
        b_start = current_shift_data.shift_b_start.time()
        b_end = current_shift_data.shift_b_end.time()
        if b_start <= now < b_end:
            shift = "B"

    # ---- SHIFT C (cross-midnight handling) ----
    if not shift and current_shift_data.shift_c_start and current_shift_data.shift_c_end:
        c_start = current_shift_data.shift_c_start.time()
        c_end = current_shift_data.shift_c_end.time()

        # **Cross-midnight condition**
        if c_start <= now or now < c_end:
            shift = "C"

    # Default
    if not shift:
        shift = "No_shift_data"

    time_ = (datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M:%S")
    log.info(f"[-] shift : {shift} and time_ : {time_}")
    return {"shift": shift, "time_": time_}

# ? Helper Function (Handles both normal + cross-midnight)
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


@router.get("/get_current_shift_data/")
async def get_current_shift_data(db: Session = Depends(get_db)):
    try:
        # ? Get latest shift configuration
        current_shift_data = (
            db.query(models.ShiftMaster)
            .order_by(models.ShiftMaster.id.desc())
            .first()
        )

        if not current_shift_data:
            return {
                "shift": "No_shift_data",
                "time_": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            }

        # ? Convert UTC ? IST
        now_utc = datetime.utcnow()
        now_ist = now_utc + timedelta(hours=5, minutes=30)
        now_time = now_ist.time()

        shift = None

        # ---------------- SHIFT A ----------------
        if current_shift_data.shift_a_start and current_shift_data.shift_a_end:
            a_start = current_shift_data.shift_a_start.time()
            a_end = current_shift_data.shift_a_end.time()

            if is_in_shift(a_start, a_end, now_time):
                shift = "A"

        # ---------------- SHIFT B ----------------
        if not shift and current_shift_data.shift_b_start and current_shift_data.shift_b_end:
            b_start = current_shift_data.shift_b_start.time()
            b_end = current_shift_data.shift_b_end.time()

            if is_in_shift(b_start, b_end, now_time):
                shift = "B"

        # ---------------- SHIFT C ----------------
        if not shift and current_shift_data.shift_c_start and current_shift_data.shift_c_end:
            c_start = current_shift_data.shift_c_start.time()
            c_end = current_shift_data.shift_c_end.time()

            if is_in_shift(c_start, c_end, now_time):
                shift = "C"

        # ? Default fallback
        if not shift:
            shift = "No_shift_data"

        return {
            "shift": shift,
            "time_": now_ist.strftime("%Y-%m-%d %H:%M:%S")
        }

    except Exception as e:
        log.error(f"[ERROR] get_current_shift_data: {str(e)}")
        return {
            "shift": "Error",
            "time_": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "message": str(e)
        }

@router.delete("/delete_shift/")
async def delete_shift(shift: str, db: Session = Depends(get_db)):
    # Fetch the last updated row
    current_shift_data = db.query(models.ShiftMaster).order_by(models.ShiftMaster.id.desc()).first()
    if not current_shift_data:
        raise HTTPException(status_code=404, detail="No shift data available")

    if shift == 'A':
        current_shift_data.shift_a_start = None
        current_shift_data.shift_a_end = None
        db.commit()
    elif shift == 'B':
        current_shift_data.shift_b_start = None
        current_shift_data.shift_b_end = None
        db.commit()
    elif shift == 'C':
        current_shift_data.shift_c_start = None
        current_shift_data.shift_c_end = None
        db.commit()
    elif shift == 'ALL_SHIFT':
        db.delete(current_shift_data)
        db.commit()
    else:
        raise HTTPException(status_code=400, detail="Invalid shift string")

    return {"detail": f"Shift {shift} data deleted successfully"}


#### CREATE DYNAMIC SHIFTS
async def create_shift_data_new(db: Session, shift_data: schemas.ShiftMasterBase):
    shifts = ['shift_a', 'shift_b', 'shift_c']

    # Retrieve the date from the provided shift data
    current_date = None
    for shift in shifts:
        start_time = getattr(shift_data, f"{shift}_start", None)
        if start_time:
            current_date = start_time.date()
            break  # Stop at the first valid start time

    if not current_date:
        raise HTTPException(status_code=400, detail="No valid shift start times provided.")

    formatted_shift_data = {
        f"{shift}_start": getattr(shift_data, f"{shift}_start", None).strftime("%Y-%m-%d %H:%M:00") if getattr(
            shift_data, f"{shift}_start", None) else None for shift in shifts}
    formatted_shift_data.update({f"{shift}_end": getattr(shift_data, f"{shift}_end", None).strftime(
        "%Y-%m-%d %H:%M:00") if getattr(shift_data, f"{shift}_end", None) else None for shift in shifts})

    # Fetch the last row data for shift_a timings if shift_b_start is provided but shift_a_start is not
    if formatted_shift_data['shift_b_start'] and not formatted_shift_data['shift_a_start']:
        last_shift = db.query(models.ShiftMaster).order_by(models.ShiftMaster.id.desc()).first()
        if last_shift:
            formatted_shift_data['shift_a_start'] = last_shift.shift_a_start.strftime("%Y-%m-%d %H:%M:00")
            formatted_shift_data['shift_a_end'] = last_shift.shift_a_end.strftime("%Y-%m-%d %H:%M:00")

            # Check if the timings match
            if (formatted_shift_data['shift_a_start'] == formatted_shift_data['shift_b_start'] and
                    formatted_shift_data['shift_a_end'] == formatted_shift_data['shift_b_end']):
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Shift A (start: {formatted_shift_data['shift_a_start']}, end: {formatted_shift_data['shift_a_end']}) "
                        f"and Shift B (start: {formatted_shift_data['shift_b_start']}, end: {formatted_shift_data['shift_b_end']}) "
                        "timings cannot be the same."
                    )
                )

            # Check if shift_b_start is less than shift_a_end
            if formatted_shift_data['shift_b_start'] < formatted_shift_data['shift_a_end']:
                raise HTTPException(status_code=400, detail="Shift B start time must be greater than Shift A end time.")

    # Fetch the last row data for shift_b timings if shift_c_start is provided but shift_b_start is not
    if formatted_shift_data['shift_c_start'] and not formatted_shift_data['shift_b_start']:
        last_shift = db.query(models.ShiftMaster).order_by(models.ShiftMaster.id.desc()).first()
        if last_shift:
            formatted_shift_data['shift_b_start'] = last_shift.shift_b_start.strftime("%Y-%m-%d %H:%M:00")
            formatted_shift_data['shift_b_end'] = last_shift.shift_b_end.strftime("%Y-%m-%d %H:%M:00")
            formatted_shift_data['shift_a_start'] = last_shift.shift_a_start.strftime("%Y-%m-%d %H:%M:00")
            formatted_shift_data['shift_a_end'] = last_shift.shift_a_end.strftime("%Y-%m-%d %H:%M:00")

            # Check if the timings match
            if (formatted_shift_data['shift_b_start'] == formatted_shift_data['shift_c_start'] and
                    formatted_shift_data['shift_b_end'] == formatted_shift_data['shift_c_end']):
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Shift B (start: {formatted_shift_data['shift_b_start']}, end: {formatted_shift_data['shift_b_end']}) "
                        f"and Shift C (start: {formatted_shift_data['shift_c_start']}, end: {formatted_shift_data['shift_c_end']}) "
                        "timings cannot be the same."
                    )
                )

            # Check if shift_c_start is less than shift_b_end
            if formatted_shift_data['shift_c_start'] < formatted_shift_data['shift_b_end']:
                raise HTTPException(status_code=400,
                                    detail="Shift C start time must be greater than Shift B end time.")

    # Check if start times are in sequence: shift_b_start >= shift_a_end and shift_c_start >= shift_b_end
    if (formatted_shift_data['shift_b_start'] and formatted_shift_data['shift_a_end'] and
            formatted_shift_data['shift_b_start'] < formatted_shift_data['shift_a_end']):
        raise HTTPException(status_code=400,
                            detail="Shift B start time must be greater than or equal to Shift A end time.")

    if (formatted_shift_data['shift_c_start'] and formatted_shift_data['shift_b_end'] and
            formatted_shift_data['shift_c_start'] < formatted_shift_data['shift_b_end']):
        raise HTTPException(status_code=400,
                            detail="Shift C start time must be greater than or equal to Shift B end time.")

    db_shift_data = models.ShiftMaster(**formatted_shift_data)

    # Validate shift times
    validate_shift_times(db_shift_data)

    db.add(db_shift_data)
    db.commit()
    db.refresh(db_shift_data)
    return db_shift_data


def validate_shift_times(shift_data: models.ShiftMaster):
    shifts = ['shift_a', 'shift_b', 'shift_c']

    # Check if any shift has the same start and end time, and ensure end time is not less than start time
    for shift in shifts:
        start_time = getattr(shift_data, f"{shift}_start", None)
        end_time = getattr(shift_data, f"{shift}_end", None)
        if start_time and end_time:
            if start_time == end_time:
                raise HTTPException(status_code=400,
                                    detail=f"Start time and end time should not be the same for {shift.capitalize()}.")
            if end_time < start_time:
                raise HTTPException(status_code=400,
                                    detail=f"End time of {shift.capitalize()} cannot be less than its start time.")

    # Ensure that shift_b starts after or at the same time as shift_a ends, and shift_c starts after or at the same time as shift_b ends
    for i in range(1, len(shifts)):
        previous_shift_end = getattr(shift_data, f"{shifts[i - 1]}_end", None)
        current_shift_start = getattr(shift_data, f"{shifts[i]}_start", None)
        if previous_shift_end and current_shift_start:
            if current_shift_start < previous_shift_end:
                raise HTTPException(status_code=400,
                                    detail=f"Start time of {shifts[i].capitalize()} must be greater than or equal to end time of {shifts[i - 1].capitalize()}.")


async def get_shift_details(db: Session):
    current_shift_data = db.query(models.ShiftMaster).order_by(models.ShiftMaster.id.desc()).first()
    if current_shift_data:
        shift_data = {}

        if current_shift_data.shift_a_start:
            shift_data["shift_a_start"] = current_shift_data.shift_a_start.strftime("%H:%M:%S")
        if current_shift_data.shift_a_end:
            shift_data["shift_a_end"] = current_shift_data.shift_a_end.strftime("%H:%M:%S")
        if current_shift_data.shift_b_start:
            shift_data["shift_b_start"] = current_shift_data.shift_b_start.strftime("%H:%M:%S")
        if current_shift_data.shift_b_end:
            shift_data["shift_b_end"] = current_shift_data.shift_b_end.strftime("%H:%M:%S")
        if current_shift_data.shift_c_start:
            shift_data["shift_c_start"] = current_shift_data.shift_c_start.strftime("%H:%M:%S")
        if current_shift_data.shift_c_end:
            shift_data["shift_c_end"] = current_shift_data.shift_c_end.strftime("%H:%M:%S")

        if shift_data:
            return shift_data
        else:
            return {"message": "No valid shift times available."}
    else:
        return {"message": "No shift data available. Please create shift data."}


async def calculate_adjusted_date(shift_a_start_str: str, time_: datetime) -> date:
    shift_a_start = datetime.strptime(shift_a_start_str, "%H:%M:%S").time()
    adjusted_datetime = time_ - timedelta(
        hours=shift_a_start.hour,
        minutes=shift_a_start.minute
    )
    return adjusted_datetime.date()
