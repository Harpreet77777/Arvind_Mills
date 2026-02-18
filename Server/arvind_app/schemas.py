from typing import Union, Optional, Dict
from datetime import date, datetime, time, timedelta
from pydantic import BaseModel


class RunCategoryBase(BaseModel):
    run_num: int
    name: str


class RunCategory(RunCategoryBase):
    run_id: int

    class Config:
        orm_mode = True


class StopCategoryBase(BaseModel):
    stop_num: int
    name: str


class StopCategory(StopCategoryBase):
    stop_id: int

    class Config:
        orm_mode = True


class OperatorListBase(BaseModel):
    operator_num: int
    name: str

    class Config:
        orm_mode = True


class OperatorList(OperatorListBase):
    operator_id: int

    class Config:
        orm_mode = True


class RunDataBase(BaseModel):
    machine: str
    run_data_id: int
    date_: date
    shift: str
    time_: datetime
    start_time: datetime
    stop_time: Union[datetime, None] = None
    duration: Union[int, None] = None
    meters: float = 0
    energy_start: float = 0
    energy_stop: Union[float, None] = None
    fluid_total: float = 0
    air_total: float = 0
    water_total: float = 0
    steam_total: Optional[float] = 0
    run_category: Union[str, None] = None
    operator_name: Union[str, None] = None
    po_number: Union[str, None] = None
    operation_name: Optional[str] = None


class RunDataCreate(RunDataBase):
    pass


class RunDataUpdate(BaseModel):
    machine: str
    run_data_id: int
    time_: datetime
    stop_time: Optional[datetime] = None
    duration: Optional[int] = None
    meters: Optional[float] = None
    energy_stop: Optional[float] = None
    fluid_total: Optional[float] = None
    air_total: Optional[float] = None
    water_total: Optional[float] = None
    steam_total: Optional[float] = None

    class Config:
        orm_mode = True


class RunData(RunDataBase):
    id: int

    class Config:
        orm_mode = True


class RunDataByPO(BaseModel):
    date_: date
    end_time: datetime
    machine: str
    po_number: str
    shift: str
    production: Union[float, None] = 0

    class Config:
        orm_mode = True


class StopDataBase(BaseModel):
    machine: str
    stop_data_id: int
    date_: date
    shift: str
    time_: datetime
    start_time: datetime
    stop_time: Union[datetime, None] = None
    duration: Union[int, None] = None
    energy_start: float
    energy_stop: Union[float, None] = None
    fluid_total: float = 0
    air_total: float = 0
    water_total: float = 0
    steam_total: Optional[float] = 0
    stop_category: Union[str, None] = None
    operator_name: Union[str, None] = None
    po_number: Union[str, None] = None


class StopDataCreate(StopDataBase):
    pass


class StopDataUpdate(BaseModel):
    machine: str
    stop_data_id: int
    time_: datetime
    stop_time: Optional[datetime] = None
    duration: Optional[int] = None
    energy_stop: Optional[float] = None
    fluid_total: Optional[float] = None
    air_total: Optional[float] = None
    water_total: Optional[float] = None
    steam_total: Optional[float] = None

    class Config:
        orm_mode = True


class StopDataByCategory(BaseModel):
    machine: str
    stop_category: str
    duration_sum: Union[int, None] = 0
    stoppage_count: int = 0

    class Config:
        orm_mode = True


class StopData(StopDataBase):
    id: int

    class Config:
        orm_mode = True


class NowProduction(BaseModel):
    production: list

    class Config:
        orm_mode = True


class NowStoppage(BaseModel):
    stoppage: list

    class Config:
        orm_mode = True


class PoDataBase(BaseModel):
    po_id: int
    po_number: Union[str, None] = None
    article: str
    greige_glm: float
    finish_glm: float
    construction: str
    hmi_data: Dict = {}
    machine: str
    plant_name: str


class PoDataCreate(PoDataBase):
    pass


class PoData(PoDataBase):
    id: int

    class Config:
        orm_mode = True


##..............................................................................


class EmailListBase(BaseModel):
    email_id_list: str


class EmailListCreate(BaseModel):
    section: str
    email_id_list: list[EmailListBase]


class EmailList(EmailListBase):
    id: int


class EmailListUpdate(BaseModel):
    email_id_list: Optional[list[str]]

    class Config:
        orm_mode = True

    class Config:
        from_attributes = True
