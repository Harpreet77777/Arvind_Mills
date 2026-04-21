from sqlalchemy import Column, Boolean, ForeignKey, Integer, String, Float, Date, DateTime, UniqueConstraint, select, \
    func, JSON
from sqlalchemy.dialects.mysql import VARCHAR

from .database import Base


class PoData(Base):
    __tablename__ = "po_data"

    id = Column(Integer, primary_key=True)
    machine_name = Column(VARCHAR(50), nullable=False)
    section = Column(VARCHAR(50), nullable=True)
    line = Column(VARCHAR(50), nullable=True)
    date_ = Column(Date)
    shift = Column(VARCHAR(1))
    po_uuid = Column(VARCHAR(50), unique=True)
    po_number = Column(VARCHAR(50), nullable=True)
    category = Column(VARCHAR(50))
    operation = Column(VARCHAR(20), nullable=True)
    start_time = Column(DateTime)
    stop_time = Column(DateTime, nullable=True)
    duration = Column(Integer, nullable=True)
    target_length = Column(Float)
    target_unit = Column(VARCHAR(50))
    machine_speed = Column(Float)
    machine_speed_unit = Column(VARCHAR(50))
    additional_data = Column(JSON)
    is_partial_gr = Column(Boolean, nullable=True)  # if False then Partial GR
    is_complete = Column(Boolean, default=False)
    operator_name = Column(VARCHAR(50))


class HourlyData(Base):
    __tablename__ = "hourly_data"

    id = Column(Integer, primary_key=True)
    machine_name = Column(VARCHAR(50), nullable=False)
    section = Column(VARCHAR(50), nullable=True)
    line = Column(VARCHAR(50), nullable=True)
    date_ = Column(Date)
    shift = Column(VARCHAR(1))
    hour = Column(Integer)
    po_uuid = Column(VARCHAR(50))
    created_at = Column(DateTime)
    updated_at = Column(DateTime, nullable=True)
    key = Column(VARCHAR(50))
    key_start = Column(Float)
    key_stop = Column(Float)
    difference_value = Column(Float)


class ShiftMaster(Base):
    __tablename__ = "shift_master"

    id = Column(Integer, primary_key=True, index=True)
    shift_a_start = Column(DateTime, default=None, nullable=True)
    shift_b_start = Column(DateTime, default=None, nullable=True)
    shift_c_start = Column(DateTime, default=None, nullable=True)
    shift_a_end = Column(DateTime, default=None, nullable=True)
    shift_b_end = Column(DateTime, default=None, nullable=True)
    shift_c_end = Column(DateTime, default=None, nullable=True)


class PlannedBreakData(Base):
    __tablename__ = 'plannedbreak_data'

    id = Column(Integer, primary_key=True, index=True)
    shift_a_planned_break = Column(JSON, default={})
    shift_b_planned_break = Column(JSON, default={})
    shift_c_planned_break = Column(JSON, default={})
    shift_g_planned_break = Column(JSON, default={})
    line = Column(String)
    machine_name = Column(String)


class OperationMaster(Base):
    __tablename__ = 'operation_master'

    id = Column(Integer, primary_key=True, index=True)
    category = Column(VARCHAR(50))
    operation = Column(VARCHAR(50))


class TargetRecord(Base):
    __tablename__ = "target_record"

    id = Column(Integer, primary_key=True, index=True)
    date_ = Column(Date)
    time_ = Column(DateTime)
    target = Column(Float)
    line = Column(String)
    shift = Column(String)
    machine = Column(String)

# ............................Breakdown..........................
class BreakdownData(Base):
    __tablename__ = "breakdown_data"

    id = Column(Integer, primary_key=True, index=True)
    date_ = Column(Date)
    shift = Column(String)
    machine_name = Column(String)
    line = Column(String)
    start_time = Column(DateTime)
    stop_time = Column(DateTime, nullable=True)
    duration = Column(Integer)
    breakdown_po_uuid = Column(VARCHAR(50))
    category = Column(String, nullable=True)
    reason = Column(String, nullable=True)

class Quality(Base):
    __tablename__ = "quality"

    id = Column(Integer, primary_key=True, index=True)
    date_ = Column(Date)
    shift = Column(String)
    machine_name = Column(String)
    line = Column(String)
    po_number = Column(VARCHAR(50), nullable=True)
    key = Column(String,nullable=False)
    value = Column(Float(50))
    value_unit = Column(VARCHAR(50))