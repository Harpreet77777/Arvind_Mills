from fastapi import Depends, APIRouter

# from .backend import get_current_po
from .. import schemas, models
from ..database import SessionLocal
from datetime import date, datetime, timedelta
from typing import List, Dict, Literal
from fastapi import HTTPException, status
from collections import defaultdict
import logging
import pytz
from sqlalchemy import func, case, and_, or_
from sqlalchemy.orm import Session
from ..routers.shift_data import get_current_shift_data, get_shift_details_data, calculate_adjusted_date

log_level = logging.INFO

FORMAT = '%(asctime)-15s %(levelname)-8s %(module)-15s:%(lineno)-8s %(message)s'

logFormatter = logging.Formatter(FORMAT)
log = logging.getLogger("Breakdown")

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


router = APIRouter(tags=["Breakdown"], prefix="/breakdown")

IST = pytz.timezone('Asia/Kolkata')


@router.get("/")
async def get_breakdown_data(machine_name: str, line: str, page: int = 1, size: int = 10,
                             db: Session = Depends(get_db)):
    offset = (page - 1) * size
    return db.query(models.BreakdownData).filter(models.BreakdownData.machine_name == machine_name,
                                                 models.BreakdownData.line == line,
                                                 ).order_by(models.BreakdownData.id.desc()).limit(size).offset(
        offset).all()


@router.get("/get_present_breakdown/{machine_name}/{line}")
async def get_present_breakdown_data(machine_name: str, line: str, db: Session = Depends(get_db)):
    return await get_machine_breakdown_data(db=db, machine_name=machine_name, line=line)


@router.post("/start_breakdown_data/")
async def create_breakdown(break_data: schemas.BreakdownDataBase, db: Session = Depends(get_db)):
    return await start_breakdown_data(db=db, break_data=break_data)


@router.post("/stop_breakdown_data/{machine_name}/{line}")
async def stop_breakdown_data(machine_name: str, line: str,
                              db: Session = Depends(get_db)):
    return await stop_breakdown(machine_name=machine_name, line=line, db=db)


@router.post("/update_breakdown_data/{id_}")
async def update_breakdown_data(id_: int, break_data: schemas.BreakdownDataUpdate, db: Session = Depends(get_db)):
    try:
        db_break = db.get(models.BreakdownData, id_)
        if not db_break:
            raise HTTPException(status_code=404, detail="Data not found")
        for key, value in break_data.dict(exclude_unset=True).items():
            setattr(db_break, key, value)
        db.add(db_break)
        db.commit()
        db.refresh(db_break)
        return db_break
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{e}")


@router.delete("/{id_}/")
async def delete_breakdown_data(id_: int, db: Session = Depends(get_db)):
    data = db.query(models.BreakdownData).filter(models.BreakdownData.id == id_).first()
    if data is None:
        raise HTTPException(status_code=404, detail="Data not found")
    delete_data = models.BreakdownData.__table__.delete().where(models.BreakdownData.id == id_)
    db.execute(delete_data)
    db.commit()
    return {"message": "Data deleted successfully"}


@router.get("/check_filled_unfilled_breakdown_reason/{machine_name}/{line}")
async def check_filled_unfilled_breakdown_reason(machine_name: str, line: str,
                                                 status: Literal['filled', 'unfilled', 'ALL'] = 'ALL', page: int = 1,
                                                 size: int = 10, db: Session = Depends(get_db)):
    """
    Return breakdown records filtered by whether `category` is filled or not.
    - status = "filled"  => category is not null and not empty
    - status = "unfilled" => category is null or empty string
    - status = "ALL" => return all records for this machine+line
    """
    offset = (page - 1) * size
    # Base query for machine and line
    query = db.query(models.BreakdownData).filter(models.BreakdownData.machine_name == machine_name,
                                                  models.BreakdownData.line == line)
    if status == "filled":
        query = query.filter(models.BreakdownData.category.isnot(None),
                             models.BreakdownData.reason.isnot(None))
    elif status == "unfilled":
        query = query.filter(or_(models.BreakdownData.category.is_(None),
                                 models.BreakdownData.reason.is_(None)))

    return query.order_by(models.BreakdownData.id.desc()).limit(size).offset(offset).all()


async def start_breakdown_data(db: Session, break_data: schemas.BreakdownDataBase):
    # try:
    start_time = datetime.now(IST)
    shift_data = await get_shift_details_data(db=db)
    date_ = await calculate_adjusted_date(shift_data["shift_a_start"],
                                          datetime.utcnow() + timedelta(hours=5, minutes=30))
    shift = await get_current_shift_data(db=db)

    # Step 3: Check if a breakdown already exists (still not stopped)
    existing_breakdown = db.query(models.BreakdownData).filter(
        models.BreakdownData.machine_name == break_data.machine_name,
        models.BreakdownData.line == break_data.line, models.BreakdownData.stop_time.is_(None)).first()
    if existing_breakdown:
        await stop_breakdown(machine_name=break_data.machine_name, line=break_data.line, db=db)

    # Step 4: Fetch planned break data for the machine
    planned_data = db.query(models.PlannedBreakData).filter(
        models.PlannedBreakData.machine_name == break_data.machine_name,
        models.PlannedBreakData.line == break_data.line
    ).all()
    if planned_data:
        shift_key_map = {"A": "shift_a_planned_break", "B": "shift_b_planned_break",
                         "C": "shift_c_planned_break", "G": "shift_g_planned_break"}
        shift_key = shift_key_map.get(shift['shift'].upper())
        if shift_key:
            shift_breaks_dict = getattr(planned_data[0], shift_key, {})
            await validate_planned_breaks(shift_breaks_dict, start_time, break_data.category)

    current_po = db.query(models.PoData).filter(models.PoData.machine_name == break_data.machine_name,
                                                models.PoData.stop_time.is_(None)).order_by(
        models.PoData.id.desc()).first()

    # Step 5: Create new breakdown record
    new_breakdown = models.BreakdownData(date_=date_, shift=shift['shift'],
                                         start_time=datetime.utcnow() + timedelta(hours=5, minutes=30),
                                         breakdown_po_uuid=current_po.po_uuid if current_po else None,
                                         **break_data.dict(exclude={"breakdown_po_uuid"}))
    db.add(new_breakdown)
    db.commit()
    db.refresh(new_breakdown)
    return new_breakdown


async def get_breakdown_data_by(machine_name: str, line: str, db: Session = Depends(get_db)):
    return db.query(models.BreakdownData).filter(
        models.BreakdownData.machine_name == machine_name,
        models.BreakdownData.line == line, models.BreakdownData.stop_time.is_(None)
    ).first()


async def stop_breakdown(machine_name: str, line: str, db: Session = Depends(get_db)):
    breakdown_data = await get_breakdown_data_by(machine_name, line, db)
    if breakdown_data is None:
        raise HTTPException(status_code=404, detail="Breakdown data not found for the given machine, id, and line")

    db_present_breakdown = db.get(models.BreakdownData, breakdown_data.id)
    stop_time = datetime.utcnow() + timedelta(hours=5, minutes=30)
    start_time = breakdown_data.start_time
    # if start_time.tzinfo is None:
    #     start_time = IST.localize(start_time)
    # else:
    #     start_time = start_time.astimezone(IST)
    # if breakdown_data.start_time:
    #     duration = (stop_time - start_time).total_seconds()
    #     breakdown_data.duration = duration
    duration = (stop_time - start_time).total_seconds()
    setattr(db_present_breakdown, "stop_time", stop_time)
    setattr(db_present_breakdown, "duration", duration)
    db.add(db_present_breakdown)
    db.commit()
    db.refresh(db_present_breakdown)
    return db_present_breakdown


async def get_machine_breakdown_data(db: Session, machine_name: str, line: str):
    result = db.query(models.BreakdownData).filter(
        models.BreakdownData.machine_name == machine_name,
        models.BreakdownData.line == line,
        models.BreakdownData.stop_time.is_(None)
    ).order_by(models.BreakdownData.id.desc()).first()

    if result is None:
        raise HTTPException(status_code=404, detail="No breakdown data found for this machine.")

    return result


async def validate_planned_breaks(shift_breaks_dict, start_time: datetime, category: str):
    if start_time.tzinfo is not None:
        start_time = (start_time.astimezone().replace(tzinfo=None))

    if isinstance(shift_breaks_dict, dict):
        for planned_name, (start_str, duration_min) in shift_breaks_dict.items():
            try:
                start_time_obj = datetime.strptime(start_str, "%H:%M:%S").time()
                now_local = datetime.utcnow() + timedelta(hours=5, minutes=30)
                break_start_dt = datetime.combine(now_local.date(), start_time_obj)
                break_end_dt = break_start_dt + timedelta(minutes=duration_min)
                if break_start_dt <= start_time <= break_end_dt:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail=(f"Cannot start breakdown '{category}' during planned break "
                                f"'{planned_name}' ({break_start_dt.time()} - "
                                f"{break_end_dt.time()})"))
            except HTTPException:
                raise
