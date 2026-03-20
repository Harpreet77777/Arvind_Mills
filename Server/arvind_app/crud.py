from collections import defaultdict
import sqlalchemy
from . import models, schemas
from typing import Dict
from datetime import date, timedelta, datetime
import calendar
from fastapi import HTTPException
import json
from sqlalchemy import cast, Float, text
from sqlalchemy import func, extract
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import JSONB, TEXT


def create_operation_master(db: Session, operation: schemas.OperationMasterCreate):
    db_obj = models.OperationMaster(category=operation.category, operation=operation.operation)
    db.add(db_obj)
    db.commit()
    db.refresh(db_obj)
    return db_obj


def get_all_operation_masters(db: Session):
    return db.query(models.OperationMaster).order_by(models.OperationMaster.id.asc()).all()


def get_operation_master_by_id(db: Session, operation_id: int):
    return db.query(models.OperationMaster).filter(models.OperationMaster.id == operation_id).first()


def get_operations_by_category(db: Session, category: str):
    return [x.operation for x in db.query(models.OperationMaster)
            .filter(models.OperationMaster.category == category)
            .order_by(models.OperationMaster.operation.asc())
            .all()]


def get_unique_operations(db: Session):
    return [row[0] for row in db.query(models.OperationMaster.operation).distinct().order_by(models.OperationMaster.operation.asc()).all()]


def get_unique_categories(db: Session):
    return [row[0] for row in db.query(models.OperationMaster.category).distinct().order_by(models.OperationMaster.category.asc()).all()]


def update_operation_master(db: Session, operation_obj: models.OperationMaster, operation_update: schemas.OperationMasterCreate):
    operation_obj.category = operation_update.category
    operation_obj.operation = operation_update.operation
    db.add(operation_obj)
    db.commit()
    db.refresh(operation_obj)
    return operation_obj


def delete_operation_master(db: Session, operation_obj: models.OperationMaster):
    db.delete(operation_obj)
    db.commit()
    return True


