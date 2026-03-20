from typing import Union, Optional, Dict, Any
from datetime import date, datetime, time, timedelta
from pydantic import BaseModel
from enum import Enum


class ShiftEnum(str, Enum):
    A = 'A'
    B = 'B'
    C = 'C'
    ALL_SHIFT = 'ALL_SHIFT'


class RunPoBase(BaseModel):
    machine_name: str
    po_number: str
    section: str
    category: str
    operation: str
    target_length: float
    target_unit: str
    machine_speed: float
    machine_speed_unit: str
    operator_name: Optional[str | None] = None
    additional_data: Optional[Dict[str, Any] | None] = None


from pydantic import BaseModel, Field


class RawDataBase(BaseModel):
    machine_name: str
    time_: datetime
    normal_data: Dict[str, Any] = Field(default_factory=dict, alias="raw_data")

    class Config:
        populate_by_name = True


class ShiftMasterBase(BaseModel):
    shift_a_start: Optional[datetime] = None
    shift_b_start: Optional[datetime] = None
    shift_c_start: Optional[datetime] = None
    shift_a_end: Optional[datetime] = None
    shift_b_end: Optional[datetime] = None
    shift_c_end: Optional[datetime] = None


class PlannedBreakDataBase(BaseModel):
    shift_a_planned_break: Dict = {}
    shift_b_planned_break: Dict = {}
    shift_c_planned_break: Dict = {}
    shift_g_planned_break: Dict = {}
    line: str
    machine_name: str


class PlannedBreakDataCreate(PlannedBreakDataBase):
    pass


class PlannedBreakDataUpdate(BaseModel):
    shift_a_planned_break: Optional[Dict] = None
    shift_b_planned_break: Optional[Dict] = None
    shift_c_planned_break: Optional[Dict] = None
    shift_g_planned_break: Optional[Dict] = None
    line: Optional[str] = None
    machine_name: Optional[str] = None

    class Config:
        from_attributes = True


class PlannedBreakData(PlannedBreakDataBase):
    id: int

    class Config:
        from_attributes = True


class OperationMasterBase(BaseModel):
    category: str
    operation: str


class OperationMasterCreate(OperationMasterBase):
    pass


class OperationMaster(OperationMasterBase):
    id: int

    class Config:
        from_attributes = True


class TargetRecordCreate(BaseModel):
    target: float
    line: str
    machine: str


class TableRecordResponse(TargetRecordCreate):
    id: int
    shift: str
    date_: date
    time_: datetime


class MachineLatestTarget(BaseModel):
    machine: str
    target: float
    line: str

    class Config:
        from_attributes = True
