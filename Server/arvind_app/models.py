from sqlalchemy import Column, ForeignKey, Integer, String, Float, Date, DateTime, UniqueConstraint, select, func, JSON
from sqlalchemy.dialects.mysql import VARCHAR
from sqlalchemy.orm import relationship
import sqlalchemy

from .database import Base


class RunCategory(Base):
    __tablename__ = "run_category"
    run_id = Column(Integer, primary_key=True)
    run_num = Column(Integer, index=True, unique=True, nullable=False)
    name = Column(VARCHAR(50), unique=True, nullable=False)
    __table_args__ = (UniqueConstraint('run_num', 'name', name='run_category_id'),)


class StopCategory(Base):
    __tablename__ = "stop_category"

    stop_id = Column(Integer, primary_key=True)
    stop_num = Column(Integer, index=True, unique=True, nullable=False)
    name = Column(VARCHAR(50), unique=True, nullable=False)
    __table_args__ = (UniqueConstraint('stop_num', 'name', name='stop_category_id'),)


class OperatorList(Base):
    __tablename__ = "operator_list"

    operator_id = Column(Integer, primary_key=True)
    operator_num = Column(Integer, index=True, unique=True, nullable=False)
    name = Column(VARCHAR(50), unique=True, nullable=False)
    __table_args__ = (UniqueConstraint('operator_num', 'name', name='operator_list_id'),)


class RunData(Base):
    __tablename__ = "run_data"

    id = Column(Integer, primary_key=True)
    machine = Column(VARCHAR(50), nullable=False)
    run_data_id = Column(Integer, unique=False, index=True)
    date_ = Column(Date)
    shift = Column(VARCHAR(1))
    time_ = Column(DateTime)
    start_time = Column(DateTime)
    stop_time = Column(DateTime, nullable=True)
    duration = Column(Integer, nullable=True)
    meters = Column(Float)
    energy_start = Column(Float)
    energy_stop = Column(Float)
    fluid_total = Column(Float)
    air_total = Column(Float)
    water_total = Column(Float)
    steam_total = Column(Float)
    run_category = Column(VARCHAR(50))
    operator_name = Column(VARCHAR(50))
    po_number = Column(VARCHAR(50))
    operation_name = Column(VARCHAR(20), nullable=True)


class StopData(Base):
    __tablename__ = "stop_data"

    id = Column(Integer, primary_key=True)
    machine = Column(VARCHAR(50), nullable=False)
    stop_data_id = Column(Integer, unique=False, index=True)
    date_ = Column(Date)
    shift = Column(VARCHAR(1), index=True)
    time_ = Column(DateTime, index=True)
    start_time = Column(DateTime, )
    stop_time = Column(DateTime, nullable=True)
    duration = Column(Integer, nullable=True)
    energy_start = Column(Float)
    energy_stop = Column(Float)
    fluid_total = Column(Float)
    air_total = Column(Float)
    water_total = Column(Float)
    steam_total = Column(Float)
    stop_category = Column(VARCHAR(50))
    operator_name = Column(VARCHAR(50))
    po_number = Column(VARCHAR(50))


class PoData(Base):
    __tablename__ = "po_data"

    id = Column(Integer, primary_key=True)
    po_id = Column(Integer, unique=False, index=True)
    po_number = Column(VARCHAR(30), nullable=False)
    article = Column(VARCHAR(100))
    greige_glm = Column(Float)
    finish_glm = Column(Float)
    construction = Column(VARCHAR(100))
    # hmi_data = Column(VARCHAR(100), default='{}')
    hmi_data = Column(JSON, default={})  # Use JSON data type
    machine = Column(VARCHAR(50), nullable=False)
    plant_name = Column(VARCHAR(50), nullable=False)


class EmailList(Base):
    __tablename__ = "email_list"

    id = Column(Integer, primary_key=True)
    email_id_list = Column(String)
    section = Column(String)
