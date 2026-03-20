from fastapi import HTTPException
from typing import List, cast, Dict
from datetime import datetime, timedelta, date
import asyncio
import pytz
from fastapi import FastAPI, Depends, APIRouter
from fastapi.openapi.utils import status_code_ranges
from pydantic import BaseModel
from sqlalchemy import func, text
from sqlalchemy.orm import Session, aliased
from ..routers.shift_data import get_current_shift_data, get_shift_details_data, calculate_adjusted_date
from .. import models, schemas
from ..database import engine, SessionLocal
from fastapi.middleware.cors import CORSMiddleware
from ..models import TargetRecord
from ..schemas import MachineLatestTarget


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


IST = pytz.timezone('Asia/Kolkata')

router = APIRouter(tags=["Target"])


@router.post("/create_target/", response_model=schemas.TableRecordResponse)
async def create_target(target: float, line: str, machine: str, db: Session = Depends(get_db)):
    shift_data = await get_shift_details_data(db=db)
    date_ = await calculate_adjusted_date(shift_data["shift_a_start"], datetime.now(IST))
    shift = await get_current_shift_data(db=db)

    # Create and store DB entry
    db_entry = models.TargetRecord(date_=date_, time_=datetime.now(IST), shift=shift['shift'],
                                   line=line, target=target, machine=machine)
    db.add(db_entry)
    db.commit()
    db.refresh(db_entry)
    return db_entry


@router.get("/all_data/")
async def get_all_data(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    return db.query(models.TargetRecord).order_by(models.TargetRecord.date_.desc()).offset(skip).limit(limit).all()


@router.get("/target/", response_model=List[schemas.TableRecordResponse])
async def get_all_target_by_machine_and_line(machine_name: str, line: str, db: Session = Depends(get_db)):
    return db.query(models.TargetRecord).filter(models.TargetRecord.machine == machine_name,
                                                models.TargetRecord.line == line).all()


@router.get("/all_machine_latest_targets/", response_model=List[MachineLatestTarget])
async def get_latest_target_of_all_machine(db: Session = Depends(get_db)):
    machine_names = db.query(models.TargetRecord.machine).distinct().all()
    machine_names = [name[0] for name in machine_names]
    latest_records = []

    for machine in machine_names:
        record = db.query(models.TargetRecord).filter(models.TargetRecord.machine == machine).order_by(
            models.TargetRecord.id.desc()).first()
        if record:
            latest_records.append(record)
    return latest_records


@router.get("/latest_target/{machine}")
async def get_latest_target(machine: str, db: Session = Depends(get_db)):
    return db.query(models.TargetRecord).filter(models.TargetRecord.machine == machine).order_by(
        models.TargetRecord.id.desc()).first()


@router.get("/line_wise/", response_model=List[schemas.TableRecordResponse])
async def get_data_by_line(line: str, db: Session = Depends(get_db)):
    entries = db.query(models.TargetRecord).filter(models.TargetRecord.line == line).all()
    if not entries:
        raise HTTPException(status_code=404, detail="line does not exist")
    return entries


@router.delete("/delete_data/")
async def delete_data(id_: int, db: Session = Depends(get_db)):
    existing = db.query(models.TargetRecord).filter(models.TargetRecord.id == id_).delete()
    if not existing:
        raise HTTPException(status_code=404, detail="Id is not found")

    db.commit()
    return {"message": "Data deleted successfully"}
