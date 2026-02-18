from fastapi import Depends, FastAPI, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from fastapi.responses import FileResponse
from datetime import date, datetime
from .. import crud, models, schemas, ms_database, Excel_Report
from .. import ms_database as msdb
from ..database import SessionLocal, engine
import aiofiles
from pathlib import Path
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


router = APIRouter(tags=["Backend"])


@router.post("/run_category/", response_model=schemas.RunCategory)
async def create_run_category(run_category: schemas.RunCategoryBase, db: Session = Depends(get_db)):
    db_run_category = await crud.get_run_category(db, run_category.name)
    if db_run_category:
        raise HTTPException(status_code=403, detail="run category already exists.")
    return await crud.create_run_category(db, run_category)


@router.post("/stop_category/", response_model=schemas.StopCategory)
async def create_stop_category(stop_category: schemas.StopCategoryBase, db: Session = Depends(get_db)):
    db_stop_category = await crud.get_stop_category(db, stop_category.name)
    if db_stop_category:
        raise HTTPException(status_code=403, detail="stop category already exists.")
    return await crud.create_stop_category(db, stop_category)


@router.post("/operator-list/", response_model=schemas.OperatorList)
async def create_operator_list(operator_list: schemas.OperatorListBase, db: Session = Depends(get_db)):
    db_operator_list = await crud.get_operator(db, operator_list.name)
    if db_operator_list:
        raise HTTPException(status_code=403, detail="operator already exists.")
    return await crud.create_operator_list(db, operator_list)


@router.post("/run_data/", response_model=schemas.RunData)
async def create_run_data(run_data: schemas.RunDataCreate, db: Session = Depends(get_db)):
    db_run_data_c = await crud.get_run_by_id(db, run_data.run_data_id, run_data.machine)
    if db_run_data_c:
        raise HTTPException(status_code=403, detail="run data already exists.")
    return await crud.create_run_data(db, run_data)


@router.post("/update_run_data/", response_model=schemas.RunData)
async def update_run_data(run_data: schemas.RunDataUpdate, db: Session = Depends(get_db)):
    db_run_data_c = await crud.get_run_by_id(db, run_data.run_data_id, run_data.machine)
    if not db_run_data_c:
        raise HTTPException(status_code=404, detail="run data does not exists.")
    return await crud.update_run_data(db=db, run_data=run_data)


@router.post("/stop_data/", response_model=schemas.StopData)
async def create_stop_data(stop_data: schemas.StopDataCreate, db: Session = Depends(get_db)):
    db_stop_data_c = await crud.get_stop_by_id(db, stop_data.stop_data_id, stop_data.machine)
    if db_stop_data_c:
        raise HTTPException(status_code=403, detail="stop data already exists.")
    return await crud.create_stop_data(db, stop_data)


@router.post("/update_stop_data/", response_model=schemas.StopData)
async def update_stop_data(stop_data: schemas.StopDataUpdate, db: Session = Depends(get_db)):
    db_stop_data_c = await crud.get_stop_by_id(db, stop_data.stop_data_id, stop_data.machine)
    if not db_stop_data_c:
        raise HTTPException(status_code=404, detail="stop data does not exists.")
    return await crud.update_stop_data(db=db, stop_data=stop_data)


@router.get("/stop_data_by_category/{machine}/{start_date}to{end_date}",
            response_model=list[schemas.StopDataByCategory])
async def get_stop_data_by_category(machine: str, start_date: date, end_date: date, db: Session = Depends(get_db)):
    return await crud.get_stop_per_day(db=db, start_date=start_date, end_date=end_date, machine=machine)


@router.get("/run_data_by_po/{machine}/{start_date}to{end_date}", )
async def get_run_data_by_po(machine: str, start_date: date, end_date: date, db: Session = Depends(get_db)):
    return await crud.get_run_per_day_(db=db, start_date=start_date, end_date=end_date, machine=machine)


@router.get("/time_sequence/{machine}/{start_date}to{end_date}", )
async def get_time_sequence(machine: str, start_date: date, end_date: date, db: Session = Depends(get_db)):
    return await crud.get_time_sequence(db=db, start_date=start_date, end_date=end_date, machine=machine)


@router.get("/employee_login/{emp_id}/{pwd}")
async def employee_login(emp_id: str, pwd: str):
    mc = ms_database.EmployeeDBHelper()
    logged_in = mc.check_employee(emp_id, pwd)
    if logged_in:
        return {"message": True}
    else:
        return {"message": False}


@router.post("/now_production/")
async def add_now_production(production_data: schemas.NowProduction):
    staging_mc = ms_database.StagingDBHelper()
    data_added = staging_mc.addProductionData(production_data)
    if data_added:
        return {"message": True}
    else:
        return {"message": False}


@router.post("/now_stoppage/")
async def add_now_stoppage(stoppage_data: schemas.NowStoppage):
    staging_mc = ms_database.StagingDBHelper()
    data_added = staging_mc.addStoppageData(stoppage_data)
    if data_added:
        return {"message": True}
    else:
        return {"message": False}


@router.post("/upload_report/")
async def upload_report(report_file: UploadFile):
    async with aiofiles.open(f"/home/his/Server/Reports/{report_file.filename}", 'wb') as out_file:
        while content := await report_file.read(1024):  # async read chunk
            await out_file.write(content)  # async write chunk
    print("Result is OK")
    return {"Result": "OK"}


@router.get("/generate_daily_report/{machine}/{date_}/{shift}", response_class=FileResponse)
async def generate_daily_report(date_: date, machine: str, shift: str):
    path_ = Path(f"/home/his/Server/Reports/AT1-{machine} Mc Report-{date_}-{shift}.xlsx")
    log.info(path_)
    if path_.exists():
        return path_
    else:
        raise HTTPException(status_code=402, detail=f"File does not exists")


@router.post("/create_po_data/")
async def create_po_data(data: schemas.PoDataBase, db: Session = Depends(get_db)):
    db_graph_data = await crud.get_po_number(db, data.machine, data.po_number, data.plant_name)

    if db_graph_data:
        return await crud.update_po_data(db=db, data=data)
    return await crud.create_po_data(db=db, podata_list=data)


@router.post("/create_email_list/")
async def create_email_list(email_list: schemas.EmailListCreate, db: Session = Depends(get_db)):
    return await crud.create_email_list(db=db, email_list=email_list)


@router.post("/update_email_list/{id}/")
async def update_email_list(id: int, email_data: schemas.EmailListUpdate, db: Session = Depends(get_db)):
    return await crud.update_email_list(db=db, email_data=email_data, id=id)


@router.get("/get_email_list_by_section/{section}/")
async def get_email_list_by_section(section: str, db: Session = Depends(get_db)):
    return await crud.get_email_list_by_section(db=db, section=section)


@router.get("/get_all_email_list/")
async def get_all_email_list(db: Session = Depends(get_db)):
    return await crud.get_all_email_list(db=db)


@router.get("/get_id_list_by_section/{section}/")
async def get_id_list_by_section(section: str, db: Session = Depends(get_db)):
    return await crud.get_id_list_by_section(db=db, section=section)


@router.delete("/delete_email_list/{id}/")
async def delete_email_list(id: int, db: Session = Depends(get_db)):
    data = db.query(models.EmailList).filter(models.EmailList.id == id).first()
    if data is None:
        raise HTTPException(status_code=404, detail="Data not found")

    delete_data = models.EmailList.__table__.delete().where(models.EmailList.id == id)
    db.execute(delete_data)

    db.commit()
    return {"ok": "deleted successfully"}


@router.get("/get_run_per_day_by_/{machine}/{start_date}to{end_date}", )
async def get_run_per_day_by_(machine: str, start_date: date, end_date: date, db: Session = Depends(get_db)):
    return await crud.get_run_per_day(db=db, start_date=start_date, end_date=end_date, machine=machine)
