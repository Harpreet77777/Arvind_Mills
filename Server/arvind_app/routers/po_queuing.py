from fastapi import Depends, APIRouter
from .. import schemas, models
from ..database import SessionLocal
from datetime import date, datetime, timedelta
from fastapi import HTTPException, status
import logging
import pytz
from sqlalchemy.orm import Session
from ..routers.shift_data import get_current_shift_data, get_shift_details_data, calculate_adjusted_date

log_level = logging.INFO

FORMAT = '%(asctime)-15s %(levelname)-8s %(module)-15s:%(lineno)-8s %(message)s'

logFormatter = logging.Formatter(FORMAT)
log = logging.getLogger("po_queuing")

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


router = APIRouter(tags=["Po Queuing"], prefix="/po_queuing")

IST = pytz.timezone('Asia/Kolkata')


@router.get("/")
async def get_all_po_queue_data(page: int = 1, size: int = 10, db: Session = Depends(get_db)):
    offset = (page - 1) * size
    return db.query(models.PoQueueing).order_by(models.PoQueueing.id.asc()).offset(offset).limit(size).all()


@router.get("/pending/po")
async def pending_po(page: int = 1, size: int = 10, db: Session = Depends(get_db)):
    offset = (page - 1) * size
    return db.query(models.PoQueueing).filter(models.PoQueueing.status == "pending").order_by(
        models.PoQueueing.id.asc()).offset(offset).limit(size).all()


@router.post("/uplord_po")
async def uplord_po(po_queue: schemas.PoQueueing, db: Session = Depends(get_db)):
    shift_data = await get_shift_details_data(db=db)
    date_ = await calculate_adjusted_date(shift_data["shift_a_start"],
                                          datetime.utcnow() + timedelta(hours=5, minutes=30))
    shift = await get_current_shift_data(db=db)

    running_po = db.query(models.PoQueueing).filter(models.PoQueueing.status == "running").first()
    # po_status = "running" if not running_po else "pending"
    db_queuing = models.PoQueueing(**po_queue.dict(), date_=date_, shift=shift['shift'])
    db.add(db_queuing)
    db.commit()
    db.refresh(db_queuing)
    # if po_status =="running":
    #     run_payload = schemas.RunPoBase(machine_name=db_queuing.machine_name,po_number=db_queuing.po_number,
    #         section=db_queuing.section,line=db_queuing.line,category=db_queuing.category,operation=getattr(db_queuing, "operation", None),
    #         target_length=db_queuing.target_length,target_unit=getattr(db_queuing, "target_unit", None),
    #         machine_speed=db_queuing.machine_speed,machine_speed_unit=getattr(db_queuing, "machine_speed_unit", None),
    #     )
    #     await start_po(po_data=run_payload,db=db)
    return db_queuing


@router.put("/update_po/{id}", status_code=status.HTTP_200_OK)
async def update_po(id: int, po_queue: schemas.PoQueueing, db: Session = Depends(get_db)):
    po_data = db.query(models.PoQueueing).filter(models.PoQueueing.id == id).first()
    if not po_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="PO data not found")
    update_data = po_queue.dict(exclude_unset=True)

    for key, value in update_data.items():
        setattr(po_data, key, value)

    db.commit()
    db.refresh(po_data)
    return po_data


@router.get("/po_by_date")
async def get_po_by_date(from_date: date, to_date: date, shift: str = None, machine: str = None,
                         line: str = None, page: int = 1, size: int = 10, db: Session = Depends(get_db)):
    offset = (page - 1) * size
    query = db.query(models.PoQueueing).filter(models.PoQueueing.date_ >= from_date, models.PoQueueing.date_ <= to_date,
                                               models.PoQueueing.status == "done")  # Changed from True

    # Optional Filters
    if shift and shift.upper() != "ALL":
        query = query.filter(models.PoQueueing.shift == shift)

    if machine and machine.upper() != "ALL":
        query = query.filter(models.PoQueueing.machine == machine)

    if line and line.upper() != "ALL":
        query = query.filter(models.PoQueueing.line == line)

    po_data = query.order_by(models.PoQueueing.id.desc()).offset(offset).limit(size).all()
    return po_data


@router.delete("/delete_po/{id}", status_code=status.HTTP_200_OK)
async def delete_po(id: int, db: Session = Depends(get_db)):
    po_data = db.query(models.PoQueueing).filter(models.PoQueueing.id == id).first()
    if not po_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="PO data not found")
    db.delete(po_data)
    db.commit()
    return {"message": "PO deleted successfully"}


@router.get("/check_running_po_and_next_po")
async def get_running_po_and_next_po(db: Session = Depends(get_db)):
    # Get currently running PO - JOIN PoQueueing with PoData
    running_po = db.query(models.PoQueueing, models.PoData).join(models.PoData,
                                                                 models.PoQueueing.po_number == models.PoData.po_number).filter(
        models.PoQueueing.status == "running",
        models.PoData.stop_time.is_(None)).first()
    print(running_po)

    # Get next pending PO from queue
    next_po = db.query(models.PoQueueing).filter(models.PoQueueing.status == "pending").order_by(
        models.PoQueueing.id.asc()).first()
    return {
        "current_po_running": running_po[0].po_number if running_po else None,
        "next_po": next_po.po_number if next_po else None,
    }



async def get_pending_po(machine, db: Session):
    pending_po = db.query(models.PoQueueing).filter(models.PoQueueing.status == "pending",
                                                    models.PoQueueing.machine_name == machine).order_by(
        models.PoQueueing.id.asc()).all()
    return [{ "po_number": po.po_number}
                           for po in pending_po]

