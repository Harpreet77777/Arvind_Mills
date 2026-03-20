from fastapi import Depends, APIRouter, HTTPException
from ..database import SessionLocal
import logging
from .. import schemas, models
from sqlalchemy.orm import Session

log_level = logging.INFO

FORMAT = '%(asctime)-15s %(levelname)-8s %(module)-15s:%(lineno)-8s %(message)s'

logFormatter = logging.Formatter(FORMAT)
log = logging.getLogger("STAGE 1")

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


router = APIRouter(tags=["Planned Breaks"], prefix="/planned_break")


########..................................Planned Break..............................................


async def create_planned_break_(db: Session, planned_data: schemas.PlannedBreakDataBase):
    # machine_db = await get_machine_clutch(db, planned_data.machine_name)
    # machine_id = machine_db.machine_id
    db_planned_data = models.PlannedBreakData(shift_a_planned_break=planned_data.shift_a_planned_break,
                                              shift_b_planned_break=planned_data.shift_b_planned_break,
                                              shift_c_planned_break=planned_data.shift_c_planned_break,
                                              shift_g_planned_break=planned_data.shift_g_planned_break,
                                              line=planned_data.line,
                                              machine_name=planned_data.machine_name)
    # db_planned_data.machine_id = machine_id
    db.add(db_planned_data)
    db.commit()
    db.refresh(db_planned_data)
    return db_planned_data


async def get_planned_break_by(db: Session, machine_name: str, line: str):
    try:
        # machine_db = await get_machine_clutch(db, machine_name)
        # machine_id = machine_db.machine_id
        pb_data = db.query(models.PlannedBreakData).filter(models.PlannedBreakData.machine_name == machine_name,
                                                           models.PlannedBreakData.line == line
                                                           ).first()
        return pb_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{e}")


async def update_planned_break_by_(db: Session, planned_data: schemas.PlannedBreakDataBase):
    try:
        data_db = await get_planned_break_by(db, planned_data.machine_name, planned_data.line)
        mo_id = data_db.id
        db_mo = db.get(models.PlannedBreakData, mo_id)
        for key, value in planned_data.dict(exclude_unset=True).items():
            setattr(db_mo, key, value)
        db.add(db_mo)
        db.commit()
        db.refresh(db_mo)
        return db_mo

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{e}")


async def get_planned_break_(db: Session):
    try:
        data = db.query(models.PlannedBreakData).order_by(
            models.PlannedBreakData.id).all()

        if not data:
            raise HTTPException(status_code=404, detail="No planned break data found")

        result = []
        for planned_break in data:
            result.append({
                "id": planned_break.id,
                "machine_name": planned_break.machine_name,
                "line": planned_break.line,
                "A": planned_break.shift_a_planned_break,
                "B": planned_break.shift_b_planned_break,
                "C": planned_break.shift_c_planned_break,
                "G": planned_break.shift_g_planned_break
            })
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{e}")


async def get_planned_data_by_line_machine_(db: Session, line: str, machine_name: str):
    try:
        planned_break = (
            db.query(models.PlannedBreakData)
            .filter(models.PlannedBreakData.line == line,
                    models.PlannedBreakData.machine_name == machine_name)
            .first()
        )

        if not planned_break:
            raise HTTPException(status_code=404, detail="No planned break data found")

        planned_break_data = planned_break
        result = {
            "id": planned_break_data.id,
            "machine_name": planned_break_data.machine_name,
            "line": planned_break_data.line,
            "A": planned_break_data.shift_a_planned_break,
            "B": planned_break_data.shift_b_planned_break,
            "C": planned_break_data.shift_c_planned_break,
            "G": planned_break_data.shift_g_planned_break
        }

        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{e}")


async def get_planned_break_data_(db: Session, id: int):
    try:
        return db.query(models.PlannedBreakData).filter(models.PlannedBreakData.id == id).first()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{e}")


async def update_planned_break_data_(db: Session, pb_data: schemas.PlannedBreakDataUpdate, id: int):
    try:
        data_db = await get_planned_break_data_(db, id)
        if data_db is None:
            raise HTTPException(status_code=404, detail=f"ID Doesn't Exists")

        mo_id = data_db.id
        db_mo = db.get(models.PlannedBreakData, mo_id)
        for key, value in pb_data.dict(exclude_unset=True).items():
            setattr(db_mo, key, value)
        db.add(db_mo)
        db.commit()
        db.refresh(db_mo)
        return db_mo
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{e}")


#########################  PLANNED BREAK DATA ###################################

@router.post("/create_planned_break/")
async def create_planned_break(planned_data: schemas.PlannedBreakDataCreate, db: Session = Depends(get_db)):
    db_planned_data = await get_planned_break_by(db, planned_data.machine_name, planned_data.line)
    if db_planned_data:
        return await update_planned_break_by_(db=db, planned_data=planned_data)
    return await create_planned_break_(db=db, planned_data=planned_data)


@router.get("/get_planned_break/")
async def get_planned_break(db: Session = Depends(get_db)):
    planned_data = await get_planned_break_(db=db)
    return planned_data


@router.get("/get_planned_data_by_line_machine/{line}/{machine_name}/")
async def get_planned_data_by_line_machine(line: str, machine_name: str, db: Session = Depends(get_db)):
    planned_data = await get_planned_data_by_line_machine_(line=line, machine_name=machine_name, db=db)
    return planned_data


@router.post("/update_planned_break_data/{id}/")
async def update_planned_break_data(id: int, pb_data: schemas.PlannedBreakDataUpdate, db: Session = Depends(get_db)):
    return await update_planned_break_data_(db=db, pb_data=pb_data, id=id)


@router.delete("/delete_planned_break/{id}/")
async def delete_planned_break(id: int, db: Session = Depends(get_db)):
    data = db.query(models.PlannedBreakData).filter(models.PlannedBreakData.id == id).first()
    if data is None:
        raise HTTPException(status_code=404, detail="Data not found")
    delete_data = models.PlannedBreakData.__table__.delete().where(models.PlannedBreakData.id == id)
    db.execute(delete_data)
    db.commit()
    return {f"Data deleted successfully "}
