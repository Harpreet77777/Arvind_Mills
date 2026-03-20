from fastapi import Depends, APIRouter, HTTPException
from sqlalchemy.orm import Session
from .. import crud, models, schemas
from ..database import SessionLocal


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


router = APIRouter(tags=["Operation Master"], prefix="/operation_master")


@router.post("/", response_model=schemas.OperationMaster)
async def create_operation_master(operation: schemas.OperationMasterCreate, db: Session = Depends(get_db)):
    existing = db.query(models.OperationMaster).filter(
        models.OperationMaster.category == operation.category,
        models.OperationMaster.operation == operation.operation,
    ).first()
    if existing:
        raise HTTPException(status_code=400, detail="Operation already exists for this category")

    return crud.create_operation_master(db=db, operation=operation)


@router.get("/", response_model=list[schemas.OperationMaster])
async def get_all_operations(db: Session = Depends(get_db)):
    return crud.get_all_operation_masters(db=db)


@router.get("/by_category/{category}")
async def get_operations_by_category(category: str, db: Session = Depends(get_db)):
    operations = crud.get_operations_by_category(db=db, category=category)
    return {"category": category, "operations": operations}


@router.get("/unique_operations")
async def get_unique_operations(db: Session = Depends(get_db)):
    return {"unique_operations": crud.get_unique_operations(db=db)}


@router.get("/unique_category")
async def get_unique_category(db: Session = Depends(get_db)):
    return {"unique_category": crud.get_unique_categories(db=db)}


@router.put("/{operation_id}", response_model=schemas.OperationMaster)
async def update_operation(operation_id: int, operation_update: schemas.OperationMasterCreate,
                           db: Session = Depends(get_db)):
    operation_obj = crud.get_operation_master_by_id(db=db, operation_id=operation_id)
    if not operation_obj:
        raise HTTPException(status_code=404, detail="Operation not found")

    return crud.update_operation_master(db=db, operation_obj=operation_obj, operation_update=operation_update)


@router.delete("/{operation_id}")
async def delete_operation(operation_id: int, db: Session = Depends(get_db)):
    operation_obj = crud.get_operation_master_by_id(db=db, operation_id=operation_id)
    if not operation_obj:
        raise HTTPException(status_code=404, detail="Operation not found")

    crud.delete_operation_master(db=db, operation_obj=operation_obj)
    return {"detail": "Operation deleted successfully"}
