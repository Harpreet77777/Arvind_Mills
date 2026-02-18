from sqlalchemy.orm import Session
from fastapi.responses import FileResponse
from datetime import date, datetime
from ..database import SessionLocal, engine
from fastapi import Depends, APIRouter
from .. import Excel_Report
import logging

log = logging.getLogger("uvicorn")
log.setLevel(logging.INFO)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


router = APIRouter(tags=["Finishing Report"])


@router.get("/get_finishing_report_data/{date_}", response_class=FileResponse)
async def get_finishing_report_data(date_: date, db: Session = Depends(get_db)):
    try:
        path_ = await Excel_Report.get_finishing_report_data(db=db, date_=date_)

        log.info(path_)
        return path_
    except Exception as e:
        log.error(e)
        return {"ERROR": str(e)}


@router.get("/get_dyeing_report_data/{date_}", response_class=FileResponse)
async def get_dyeing_report(date_: date, db: Session = Depends(get_db)):
    try:
        print("hihi")
        path_ = await Excel_Report.generate_dyeing_report_data(db=db, date_=date_)
        print(path_)

        log.info(path_)
        return path_
    except Exception as e:
        log.error(e)
        return {"ERROR": str(e)}


@router.get("/generate_preparatory_report_data/{date_}", response_class=FileResponse)
async def generate_preparatory_report_data(date_: date, db: Session = Depends(get_db)):
    try:
        path_ = await Excel_Report.generate_preparatory_report_data(db=db, date_=date_)
        print(path_)

        log.info(path_)
        return path_
    except Exception as e:
        log.error(e)
        return {"ERROR": str(e)}


@router.get("/download_preparatory_report_data_test_11/{date_}", response_class=FileResponse)
async def download_preparatory_report_data(date_: date, db: Session = Depends(get_db)):
    try:
        path_ = await Excel_Report.download_preparatory_report_data(db=db, date_=date_)
        print(path_)

        log.info(path_)
        return path_
    except Exception as e:
        log.error(e)
        return {"ERROR": str(e)}


@router.get("/download_and_fill_preparatory_report/{date_}", response_class=FileResponse)
async def download_and_fill_preparatory_report(date_: date, db: Session = Depends(get_db)):
    try:
        path_ = await Excel_Report.download_and_fill_preparatory_report(db=db, date_=date_)
        print(path_)

        log.info(path_)
        return path_
    except Exception as e:
        log.error(e)
        return {"ERROR": str(e)}


@router.get("/get_preparatory_production_report_data/{date_}", response_class=FileResponse)
async def get_preparatory_production_report_data(date_: date, db: Session = Depends(get_db)):
    try:
        path_ = await Excel_Report.get_preparatory_production_report_data(db=db, date_=date_)
        print(path_)

        log.info(path_)
        return path_
    except Exception as e:
        log.error(e)
        return {"ERROR": str(e)}
