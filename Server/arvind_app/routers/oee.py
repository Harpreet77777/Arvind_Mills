import asyncio
import math
import copy
from typing import Optional
import pytz
from sqlalchemy import func, select
from fastapi import Depends, APIRouter, HTTPException
from sqlalchemy.orm import Session
from datetime import date, datetime, timedelta, time
from .. import schemas, models
from . import shift_data
from . shift_data import get_shift_details_data
from .quality_analysis import calculate_quantity
from ..database import SessionLocal
from typing import Tuple, Dict, Any
from ..schemas import ShiftEnum
import logging

log_level = logging.INFO

FORMAT = '%(asctime)-15s %(levelname)-8s %(module)-15s:%(lineno)-8s %(message)s'

logFormatter = logging.Formatter(FORMAT)
log = logging.getLogger("OEE")

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


router = APIRouter(tags=["OEE"], prefix="/oee")


async def get_shift_timings(db: Session):
    shift_data = await get_shift_details_data(db)
    # general_shift_data = await general_shift.get_general_shift(db)
    # pass db directly

    if not shift_data:
        log.error("Failed to fetch normal shift details.")
        return {}

    return {
        'A': (
            time.fromisoformat(shift_data.get("shift_a_start", "00:00:00")),
            time.fromisoformat(shift_data.get("shift_a_end", "23:59:59"))
        ),
        'B': (
            time.fromisoformat(shift_data.get("shift_b_start", "00:00:00")),
            time.fromisoformat(shift_data.get("shift_b_end", "23:59:59"))
        ),
        'C': (
            time.fromisoformat(shift_data.get("shift_c_start", "00:00:00")),
            time.fromisoformat(shift_data.get("shift_c_end", "23:59:59"))
        )
    }


# ✅ optional API wrapper
@router.get("/initialize_shift_timings/")
async def initialize_shift_timings_api(db: Session = Depends(get_db)):
    return await get_shift_timings(db)

@router.get("/calculate_availability/")
async def calculate_availability(date_: date, shift: schemas.ShiftEnum, machine: str, line: str,
                                 db: Session = Depends(get_db)):
    return await _calculate_availability(date_, shift, machine, line, db)


@router.get("/calculate_efficiency/")
async def calculate_efficiency(date_: date, shift: schemas.ShiftEnum, machine:str, line: str,
                               db: Session = Depends(get_db)):
    return await _calculate_efficiency(date_, shift, machine, line, db)

@router.get("/calculate_quality/")
async def calculate_quality(date_: date, shift: schemas.ShiftEnum, machine:str, line: str,
                            db: Session = Depends(get_db)):
    return await _calculate_quality(date_, shift, machine, line, db)


@router.get("/calculate_oee/")
async def calculate_oee(date_: date, shift: schemas.ShiftEnum, machine:str, line: str,
                        db: Session = Depends(get_db)):
    quality_results = await _calculate_oee(date_, shift, machine, line, db)
    return quality_results


@router.post("/calculate_part_count/")
async def calculate_part_count(date_: date, shift: schemas.ShiftEnum, machine:str, line: str,
                               db: Session = Depends(get_db)):
    availability_results = await calculate_quantity(date_, shift, machine, line, db)
    return availability_results


#######################################################################################

async def _calculate_availability(date_: date, shift: schemas.ShiftEnum, machine: str, line: str,
                                  db: Session = Depends(get_db)):
    downtimes, planned_downtimes, operating_time, total_time = await calc_availability(date_, shift, machine,
                                                                                       line, db)
    # downtimes, planned_downtimes, operating_time, total_time
    if total_time:
        total_available_time = total_time[0]['duration']
    else:
        total_available_time = 0
    ae = ""
    try:
        availability = round((operating_time / (total_available_time - planned_downtimes)) * 100, 2)
        log.debug(f"availability: {availability}, {total_available_time}, {operating_time}")
    except ArithmeticError as ae:
        availability = 0
    return {"availability": availability, 'operating_time': operating_time, 'total_time': total_available_time,
            "planned_downtimes": planned_downtimes, "downtimes": downtimes}


async def calc_availability(date_: date, shift: schemas.ShiftEnum, machine: str, line: str,
                            db: Session = Depends(get_db)):
    global shift_timings
    shift_timings = await get_shift_timings(db)
    if await cal_current_date_shift(db, date_, shift):
        if shift == schemas.ShiftEnum.ALL_SHIFT:
            shift_start_time = datetime.combine(date_, shift_timings["A"][0])
            shift_stop_time = datetime.now(pytz.timezone('Asia/Kolkata')).replace(tzinfo=None)
            total_time = [{'start_time': shift_start_time.strftime("%Y-%m-%d %H:%M:%S"),
                           'stop_time': shift_stop_time.strftime("%Y-%m-%d %H:%M:%S"),
                           'duration': (shift_stop_time - shift_start_time).total_seconds()}]
        else:
            shift_start_time = datetime.combine(date_, shift_timings[shift][0])
            shift_stop_time = datetime.now(pytz.timezone('Asia/Kolkata')).replace(tzinfo=None)
            total_time = [{'start_time': shift_start_time.strftime("%Y-%m-%d %H:%M:%S"),
                           'stop_time': shift_stop_time.strftime("%Y-%m-%d %H:%M:%S"),
                           'duration': (shift_stop_time - shift_start_time).total_seconds()}]
    else:
        if shift == schemas.ShiftEnum.ALL_SHIFT:
            shift_start_time = datetime.combine(date_, shift_timings["A"][0])
            shift_stop_time = datetime.combine(date_ + timedelta(days=1), shift_timings["C"][1])
            total_time = [{
                'start_time': shift_start_time.strftime("%Y-%m-%d %H:%M:%S"),
                'stop_time': shift_stop_time.strftime("%Y-%m-%d %H:%M:%S"),
                'duration': (shift_stop_time - shift_start_time).total_seconds()
            }]
        elif shift == schemas.ShiftEnum.B:
            shift_start_time = datetime.combine(date_, shift_timings[shift][0])
            shift_stop_time = datetime.combine(date_, shift_timings[shift][1])
            total_time = [{'start_time': shift_start_time.strftime("%Y-%m-%d %H:%M:%S"),
                           'stop_time': shift_stop_time.strftime("%Y-%m-%d %H:%M:%S"),
                           'duration': (shift_stop_time - shift_start_time).total_seconds()}]
        elif shift == schemas.ShiftEnum.C:
            shift_start_time = datetime.combine(date_, shift_timings['C'][0])
            shift_stop_time = datetime.combine(date_ + timedelta(days=1), shift_timings['C'][1])
            total_time = [{'start_time': shift_start_time.strftime("%Y-%m-%d %H:%M:%S"),
                           'stop_time': shift_stop_time.strftime("%Y-%m-%d %H:%M:%S"),
                           'duration': (shift_stop_time - shift_start_time).total_seconds()}]
        else:
            shift_start_time = datetime.combine(date_, shift_timings[shift][0])
            shift_stop_time = datetime.combine(date_, shift_timings[shift][1])
            total_time = [{'start_time': shift_start_time.strftime("%Y-%m-%d %H:%M:%S"),
                           'stop_time': shift_stop_time.strftime("%Y-%m-%d %H:%M:%S"),
                           'duration': (shift_stop_time - shift_start_time).total_seconds()}]

    log.debug(f"total time:{total_time}")
    initial_total_time = copy.deepcopy(total_time)

    planned_breaks = await get_planned_break_by_shift(machine, line, shift, db)
    subtracted_planned_total_time = subtract_planned_breaks(date_, initial_total_time, planned_breaks)

    downtimes = await get_aggregated_downtimes(date_, shift, machine, line, db)
    # planned_downtimes = await get_aggregated_planned_downtimes(date_,shift,machine,line, db)
    planned_downtimes = total_time[0]['duration'] - subtracted_planned_total_time[0]['duration']

    operating_time = total_time[0]['duration'] - downtimes - planned_downtimes
    if operating_time < 0:
        operating_time = 0
    return downtimes, planned_downtimes, operating_time, total_time


# -------------------------------------------------------------------------------------------
async def get_planned_break_by_shift(machine: str, line: str, shift: schemas.ShiftEnum,
                                     db: Session = Depends(get_db)):
    planned_breaks_db = db.query(
        models.PlannedBreakData.shift_a_planned_break,
        models.PlannedBreakData.shift_b_planned_break,
        models.PlannedBreakData.shift_c_planned_break,
        models.PlannedBreakData.shift_g_planned_break
    ).filter(models.PlannedBreakData.machine_name == machine,
             models.PlannedBreakData.line == line).first()

    log.debug(f"planned_break : {planned_breaks_db}")

    if planned_breaks_db:
        if shift == 'A':
            planned_breaks = planned_breaks_db[0]
        elif shift == 'B':
            planned_breaks = planned_breaks_db[1]
        elif shift == 'C':
            planned_breaks = planned_breaks_db[2]
        elif shift == 'G':
            planned_breaks = planned_breaks_db[3]
        else:
            planned_breaks = merge_dicts_with_suffix(planned_breaks_db)
    else:
        planned_breaks = {}

    log.debug(f"planned_break : {planned_breaks}")
    return planned_breaks


def subtract_planned_breaks(date_, model_periods, planned_break):
    base_date = date_
    shift_a_start = time(6, 00, 0, 0)
    for break_type, break_info in planned_break.items():
        break_start = datetime.strptime(f"{base_date} {break_info[0]}", "%Y-%m-%d %H:%M:%S")

        # ✅ Use shift_a_start instead of conversions.shift_a_start
        if break_start < datetime.combine(date_, shift_a_start):
            break_start = datetime.strptime(
                f"{base_date + timedelta(days=1)} {break_info[0]}",
                "%Y-%m-%d %H:%M:%S"
            )

        break_end = break_start + timedelta(minutes=break_info[1])
        planned_break[break_type] = [break_start, break_end]

    log.debug(f"Planned breaks with date: {planned_break}")

    updated_model_periods = []
    for i, period in enumerate(model_periods):
        start_time = datetime.strptime(period['start_time'], "%Y-%m-%d %H:%M:%S")
        stop_time = datetime.strptime(period['stop_time'], "%Y-%m-%d %H:%M:%S")

        for break_type, (break_start, break_end) in planned_break.items():
            if start_time < break_end and stop_time > break_start:
                overlap_start = max(start_time, break_start)
                overlap_end = min(stop_time, break_end)
                overlap_duration = (overlap_end - overlap_start).total_seconds()
                period['duration'] -= int(overlap_duration)

        updated_model_periods.append(period)

    return updated_model_periods


def merge_dicts_with_suffix(dict_list):
    result = {}
    key_counts = {}

    for d in dict_list:
        for key, value in d.items():
            if key in result:
                key_counts[key] = key_counts.get(key, 1) + 1
                new_key = f"{key}_{key_counts[key]}"
                result[new_key] = value
            else:
                result[key] = value

    return result


# -----------------------------------------------------------------------------------

async def fetch_shift_data(db: Session):
    try:
        current_shift = await shift_data.get_current_shift_data(db=db)
        shift_details = await shift_data.get_shift_details_data(db=db)

        if not current_shift:
            raise HTTPException(status_code=500, detail="Failed to fetch current shift data")
        if not shift_details:
            raise HTTPException(status_code=500, detail="Failed to fetch shift details data")

        return current_shift.get("shift"), shift_details

    finally:
        db.close()  # Close DB session if you're manually managing it


async def get_current_date_and_shift(db: Session, current_datetime: Optional[datetime] = None) -> Tuple[str, date]:
    shift, shift_data = await fetch_shift_data(db)

    # Use passed datetime or default to current system time
    if current_datetime is None:
        current_datetime = datetime.now()

    shift_a_start_str = shift_data.get("shift_a_start")
    if not shift_a_start_str:
        raise ValueError("shift_a_start not found in shift data")

    adjusted_date = calculate_adjusted_date(shift_a_start_str, current_datetime)
    return shift, adjusted_date


async def shift_a(db: Session, current_datetime: Optional[datetime] = None) -> Tuple[str, date, time]:
    shift, shift_data = await fetch_shift_data(db)

    if current_datetime is None:
        current_datetime = datetime.now()

    shift_a_start_str = shift_data.get("shift_a_start")
    if not shift_a_start_str:
        raise ValueError("shift_a_start not found in shift data")

    # ✅ Convert string to datetime.time
    shift_a_start = datetime.strptime(shift_a_start_str, "%H:%M:%S").time()

    adjusted_date = calculate_adjusted_date(shift_a_start_str, current_datetime)
    return shift, adjusted_date, shift_a_start


def calculate_adjusted_date(shift_a_start_str: str, current_datetime: datetime) -> date:
    shift_a_start = datetime.strptime(shift_a_start_str, "%H:%M:%S").time()
    adjusted_datetime = current_datetime - timedelta(
        hours=shift_a_start.hour,
        minutes=shift_a_start.minute
    )
    return adjusted_datetime.date()


async def cal_current_date_shift(db: Session, date_: date, shift: schemas.ShiftEnum) -> bool:
    current_shift, current_date = await get_current_date_and_shift(db)

    if shift == schemas.ShiftEnum.ALL_SHIFT:
        return current_date == date_

    return current_date == date_ and current_shift == shift

async def get_aggregated_downtimes(date_: date, shift: schemas.ShiftEnum, machine:str, line: str,
                                   db: Session):
    # machine_id = await crud.get_machine_id(machine_name=e_params.machine, db=db)
    if shift == "ALL_SHIFT":
        downtimes_db = db.query(func.sum(models.BreakdownData.duration)
                                ).filter(models.BreakdownData.date_ == date_,
                                         models.BreakdownData.machine_name == machine,
                                         models.BreakdownData.line == line,
                                         models.BreakdownData.stop_time.isnot(None)
                                         ).first()
    else:
        downtimes_db = db.query(func.sum(models.BreakdownData.duration)
                                ).filter(models.BreakdownData.date_ == date_,
                                         models.BreakdownData.shift == shift,
                                         models.BreakdownData.machine_name == machine,
                                         models.BreakdownData.line == line,
                                         models.BreakdownData.stop_time.isnot(None)
                                         ).first()
    if downtimes_db[0] is None:
        close_downtime = 0
    else:
        close_downtime = downtimes_db[0]
    if shift == "ALL_SHIFT":
        open_breakdown = db.query(models.BreakdownData).filter(models.BreakdownData.date_ == date_,
                                                               models.BreakdownData.machine_name == machine,
                                                               models.BreakdownData.line == line,
                                                               models.BreakdownData.stop_time.is_(None)
                                                               ).first()
    else:
        open_breakdown = db.query(models.BreakdownData).filter(models.BreakdownData.date_ == date_,
                                                               models.BreakdownData.shift == shift,
                                                               models.BreakdownData.machine_name == machine,
                                                               models.BreakdownData.line == line,
                                                               models.BreakdownData.stop_time.is_(None)
                                                               ).first()
    if open_breakdown is None:
        open_downtime = 0
    else:
        stop_time_naive = datetime.utcnow()
        # timezone = pytz.timezone('Asia/Kolkata')
        # stop_time = timezone.localize(stop_time_naive.replace(tzinfo=None))
        open_downtime = (stop_time_naive - open_breakdown.start_time).total_seconds()
    combined_downtime = open_downtime + close_downtime
    log.debug(f"downtimes: {combined_downtime}")
    return combined_downtime


async def get_earliest_target_data(machine:str, line: str, db: Session) -> dict:
    """Get the earliest target record for a given machine and line."""
    fallback_record = db.query(models.TargetRecord).filter(
        models.TargetRecord.machine == machine,
        models.TargetRecord.line == line
    ).order_by(models.TargetRecord.date_.asc(), models.TargetRecord.id.asc()).first()

    if not fallback_record:
        raise HTTPException(status_code=404, detail="No target data found for this machine and line")

    return {
        "target": fallback_record.target,
        "date_": fallback_record.date_,
        "machine_name": fallback_record.machine
    }


async def get_target_data_by_machine(machine:str, line: str, date_: date = None,
                                     db: Session = Depends(get_db)):
    # First try to get the most recent record matching the criteria
    query = db.query(models.TargetRecord).filter(models.TargetRecord.machine == machine,
                                                  models.TargetRecord.line == line)

    if date_:
        query = query.filter(models.TargetRecord.date_ <= date_)

    record = query.order_by(models.TargetRecord.date_.desc(), models.TargetRecord.id.desc()).first()

    if not record:
        return await get_earliest_target_data(machine, line, db)

    return {
        "target": record.target,
        "date_": record.date_,
        "machine_name": record.machine
    }


async def _calculate_efficiency(date_: date, shift: schemas.ShiftEnum, machine:str, line: str,
                                db: Session = Depends(get_db)):

    # For ALL_SHIFT, sum over A, B, and C
    if shift == schemas.ShiftEnum.ALL_SHIFT:  # import here if not globally done
        result = await calculate_quantity(date_, shift, machine, line, db)
        total_ok = result["ok"]
        total_not_ok = result["not_ok"]
        actual_part = total_ok + total_not_ok
        target_data = await get_target_data_by_machine(machine=machine, line=line, date_=date_, db=db)
        ideal_cycle_time = target_data["target"]

    else:
        result = await calculate_quantity(date_, shift, machine, line, db)
        total_ok = result["ok"]
        total_not_ok = result["not_ok"]
        actual_part = total_ok + total_not_ok
        target_data = await get_target_data_by_machine(machine=machine, line=line, date_=date_, db=db)
        ideal_cycle_time = target_data["target"]

    # Get availability info
    availability_data = await _calculate_availability(date_=date_, shift=shift, machine=machine, line=line, db=db)

    total_time = availability_data['operating_time']  # in seconds
    target = math.ceil(total_time * ideal_cycle_time)

    ae = None
    try:
        log.debug(f"efficiency: {ideal_cycle_time},{actual_part}")
        efficiency = round((actual_part / target) * 100, 2)
    except ArithmeticError as ae:
        efficiency = 0
    return {"efficiency": efficiency, 'part_count': actual_part, 'target_count': target,
            'ideal_cycle_time': ideal_cycle_time, 'operating_time': total_time}


async def _calculate_oee(date_: date, shift: schemas.ShiftEnum, machine:str, line: str,
                         db: Session = Depends(get_db)):
    if not date_ or not shift or not machine or not line:
        availability_data = {
            'availability': 0,
            'operating_time': 0,
            'total_time': 0
        }
        efficiency_data = {
            'efficiency': 0,
            'part_count': 0,
            'target_count': 0
        }
        quality_data = {
            'quality': 0,
            'ok_parts': 0,
            'not_ok_parts': 0
        }
    else:
        availability_data = await _calculate_availability(date_, shift, machine, line, db)
        efficiency_data = await calculate_efficiency(date_, shift, machine, line, db)
        quality_data = await calculate_quality(date_, shift, machine, line, db)

    availability_value = availability_data.get('availability', 0)
    efficiency_value = efficiency_data.get('efficiency', 0)
    quality_value = quality_data.get('quality', 0)

    oee_value = 0
    if availability_value > 0 and efficiency_value > 0:
        oee_value = (availability_value * efficiency_value * quality_value) / 10000

    return {
        "availability": availability_value,
        "efficiency": efficiency_value,
        "quality": quality_value,
        "oee": oee_value
    }


async def _calculate_quality(date_: date, shift: schemas.ShiftEnum, machine:str, line: str,
                             db: Session):
    result = await calculate_quantity(date_, shift, machine, line, db)
    total_ok = result["ok"]
    total_not_ok = result["not_ok"]
    total_part = total_ok + total_not_ok
    try:
        quality = round((total_ok / total_part) * 100, 2) if total_part > 0 else 0
    except ArithmeticError:
        quality = 0

    return {
        "quality": quality,
        "ok_parts": total_ok,
        "not_ok_parts": total_not_ok
    }


# --------------------------------------------------Target Vs Actual ----------------------------------------------------
@router.get("/oee_vs_target_vs_actual_date_range/")
async def oee_vs_target_vs_actual(from_date: date, to_date: date, line: str, shift: schemas.ShiftEnum,
                                  machine:str, db: Session = Depends(get_db)):
    results = []
    current_date = from_date

    while current_date <= to_date:
        # Determine if we should process G shift only or A+B+C
        if shift == schemas.ShiftEnum.ALL_SHIFT:
            shift_list = [schemas.ShiftEnum.A, schemas.ShiftEnum.B, schemas.ShiftEnum.C]
        else:
            shift_list = [shift]
        # Process selected shifts
        for single_shift in shift_list:
            target_data, actual_data = await asyncio.gather(
                get_target_production(from_date=current_date, to_date=current_date, line=line, machine=machine, db=db),
                get_actual_production(db=db, from_date=current_date, to_date=current_date, line=line,
                                      shift=single_shift, machine=machine)
            )

            oee_data = await _calculate_oee(db=db, date_=current_date, line=line, shift=single_shift, machine=machine)
            availability_data = await _calculate_availability(date_=current_date, shift=single_shift, machine=machine,
                                                              line=line, db=db)

            ideal_cycle_time = target_data["target"]
            total_time = availability_data['total_time'] - availability_data['planned_downtimes']
            target = math.ceil(total_time / ideal_cycle_time) if ideal_cycle_time > 0 else 0

            results.append({
                "date": current_date.isoformat(),
                "shift": single_shift.value,
                "line": line,
                "machine": machine.value if hasattr(machine, 'value') else machine,
                "target": target,
                "actual": actual_data.get("total_parts", 0),
                "oee": oee_data.get("oee", 0),
                "avalibilty": oee_data.get("availability", 0),
                "efficiency": oee_data.get("efficiency", 0),
                "quality": oee_data.get("quality", 0)
            })

        current_date += timedelta(days=1)

    return results


# ------------------------------------------------------------------------------------------------------------------------------------------------

@router.get("/oee_vs_target_vs_actual/")
async def oee_vs_target_vs_actual(from_date: date, to_date: date, line: str, shift: ShiftEnum, machine: str,
                                  db: Session = Depends(get_db)):
    if from_date > to_date:
        raise HTTPException(status_code=400, detail="from_date must be before or equal to to_date")

    total_target = 0
    total_time = 0
    total_actual = 0
    ok_parts = 0
    not_ok_parts = 0
    planned_downtimes = 0
    unplanned_downtimes = 0
    operating = 0

    current_date = from_date
    while current_date <= to_date:
        used_shift =shift
        target_data = await _calculate_efficiency(date_=current_date, line=line, shift=used_shift, machine=machine,
                                                  db=db)
        total_target += target_data.get("target_count", 0)

        actual_data, _ = await asyncio.gather(
            get_actual_production(db=db, from_date=current_date, to_date=current_date, shift=used_shift,
                                  line=line, machine=machine),
            get_oee_range(db=db, from_date=current_date, to_date=current_date, shift=used_shift,
                          line=line, machine=machine)
        )

        availability_data = await _calculate_availability(db=db, date_=current_date, line=line,
                                                          shift=used_shift, machine=machine)

        operating += availability_data.get("operating_time", 0)
        total_time += availability_data.get("total_time", 0)
        planned_downtimes += availability_data.get("planned_downtimes", 0)
        unplanned_downtimes += availability_data.get("downtimes", 0)
        ok_parts += actual_data.get("ohk_parts", 0)
        not_ok_parts += actual_data.get("not_ohk_parts", 0)
        total_actual += actual_data.get("total_parts", 0)

        current_date += timedelta(days=1)

    # Metrics calculation
    ideal_cycle = (operating / total_target) if total_target else 0
    available_time = total_time - planned_downtimes
    availability = (operating / available_time * 100) if available_time else 0
    efficiency = ((ideal_cycle * total_actual) / operating * 100) if operating else 0
    quality = (ok_parts / total_actual * 100) if total_actual else 0
    oee = (availability * efficiency * quality) / 10000

    shift_str = "G where applicable else whole-day" if shift == ShiftEnum.ALL_SHIFT else shift.value

    response_item = {
        "date_range": f"{from_date.strftime('%Y-%m-%d')} to {to_date.strftime('%Y-%m-%d')}",
        "line": line,
        "shift": shift_str,
        "machine": machine,
        "total_target": total_target,
        "total_time": total_time,
        "actual": total_actual,
        "ok_parts": ok_parts,
        "not_ok_parts": not_ok_parts,
        "operating_time": operating,
        "planned_downtimes": planned_downtimes,
        "unplanned_downtimes": unplanned_downtimes,
        "availability": availability,
        "efficiency": efficiency,
        "quality": quality,
        "oee": oee,
        "total_days": (to_date - from_date).days + 1,
    }

    return [response_item]


# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

async def get_actual_production(db: Session, from_date: date,
                                to_date: date, line: str, shift: ShiftEnum, machine: str) -> Dict[str, Any]:
    try:
        current_date = from_date
        total_ok_parts = 0
        total_not_ok_parts = 0

        while current_date <= to_date:
            daily_quality = await _calculate_quality(current_date, shift, machine, line, db)
            total_ok_parts += daily_quality["ok_parts"]
            total_not_ok_parts += daily_quality["not_ok_parts"]
            current_date += timedelta(days=1)

        return {
            "ohk_parts": total_ok_parts,
            "not_ohk_parts": total_not_ok_parts,
            "total_parts": total_ok_parts + total_not_ok_parts
        }

    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Error calculating actual production: {str(e)}"
        )


async def get_target_production(from_date: date, to_date: date, line: str, machine:str,
                                db: Session) -> Dict[str, Any]:
    # First get the most recent target before the end date
    try:
        target_data = await get_target_data_by_machine(machine=machine, line=line, date_=to_date,
                                                       db=db)  # Get most recent target up to end date
        current_target = target_data['target']
    except HTTPException:
        current_target = 0  # Default if no target found

    return {
        "from_date": from_date,
        "to_date": to_date,
        "line": line,
        "machine": machine.value if hasattr(machine, 'value') else machine,
        "target": current_target
    }


async def get_oee_range(db: Session, from_date: date, to_date: date, line: str,
                        shift: ShiftEnum, machine: str) -> Dict[str, Any]:
    try:
        current_date = from_date
        total_availability = 0
        total_efficiency = 0
        total_quality = 0
        total_oee = 0
        valid_days = 0

        while current_date <= to_date:
            try:
                daily_oee = await _calculate_oee(current_date, shift, machine, line, db)
                total_availability += daily_oee["availability"]
                total_efficiency += daily_oee["efficiency"]
                total_quality += daily_oee["quality"]
                total_oee += daily_oee["oee"]
                valid_days += 1
            except Exception:
                # Skip this day if there's any issue in fetching data
                pass

            current_date += timedelta(days=1)

        if valid_days == 0:
            return {
                "availability": 0,
                "efficiency": 0,
                "quality": 0,
                "oee": 0
            }

        return {
            "availability": round(total_availability / valid_days, 2),
            "efficiency": round(total_efficiency / valid_days, 2),
            "quality": round(total_quality / valid_days, 2),
            "oee": round(total_oee / valid_days, 2),
            "total": valid_days
        }

    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Error calculating OEE for date range: {str(e)}"
        )


async def get_oee_date_range(
        db: Session,
        from_date: date,
        to_date: date,
        line: str,
        shift: ShiftEnum,
        machine: str
):
    try:
        current_date = from_date
        oee_results = []

        while current_date <= to_date:
            try:
                daily_oee = await _calculate_oee(current_date, shift, machine, line, db)
                oee_results.append({
                    "date_": current_date,
                    "availability": round(daily_oee["availability"], 2),
                    "efficiency": round(daily_oee["efficiency"], 2),
                    "quality": round(daily_oee["quality"], 2),
                    "oee": round(daily_oee["oee"], 2)
                })
            except Exception:
                # Add zeroed result or skip (choose one)
                oee_results.append({
                    "date_": current_date,
                    "availability": 0,
                    "efficiency": 0,
                    "quality": 0,
                    "oee": 0
                })

            current_date += timedelta(days=1)

        return oee_results

    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Error calculating OEE for date range: {str(e)}"
        )


@router.get("/averge/oee_linewise/{date_}/{shift}/{line}")
async def get_averge_oee_linewise(date_: date, shift: schemas.ShiftEnum, line: str, db: Session = Depends(get_db)):
    availability = 0
    efficiency = 0
    quality = 0
    oee = 0
    total = 0
    machine_list = ["Pole Assembly","Base Assembly", "Smart Screw Tightening", "Force Test Bench","MV", "MT", "Thermal 1",
                    "Cover Assembly", "Finishing Section", "HV", "QA", "Pick & Pack"]
    for m in machine_list:
        averge_oee = await calculate_oee(date_=date_, shift=shift, machine=m, line=line, db=db)
        availability += averge_oee.get("availability", 0)
        efficiency = averge_oee.get("efficiency", 0)
        quality = averge_oee.get("quality", 0)
        oee = averge_oee.get("oee", 0)
        total += 1

    if total == 0:
        return {"availability": 0, "efficiency": 0, "quality": 0, "oee": 0}

    return {"date_":date_,"line":line,"availability": round(availability / total, 2), "efficiency": round(efficiency / total, 2),
            "quality": round(quality / total, 2), "oee": round(oee / total, 2)}
