from typing import Dict, Tuple, List, Optional
from fastapi import Depends, FastAPI, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from fastapi.responses import FileResponse
from sqlalchemy import cast, Time, or_, and_
from datetime import date, datetime, timedelta, time
from .. import crud, models, schemas
from ..database import SessionLocal, engine
import uuid
import pytz
from pathlib import Path
from fastapi import Depends, APIRouter
import logging

log = logging.getLogger("uvicorn")
log.setLevel(logging.INFO)

IST = pytz.timezone('Asia/Kolkata')


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


router = APIRouter(tags=["Analytics"])


@router.get("/get_po_data/{from_date}/{to_date}")
async def get_po_data(from_date: date, to_date: date, db: Session = Depends(get_db)):
    po_data = db.query(models.PoData).filter(models.PoData.date_.between(from_date, to_date),
                                             models.PoData.stop_time != None).order_by(models.PoData.id.desc()).all()
    return [{"po_number": data.po_number,
             "machine_name": data.machine_name,
             "po_uuid": data.po_uuid,
             "start_time": data.start_time,
             "stop_time": data.stop_time
             } for data in po_data]


async def calculate_key_value(po_uuid: uuid, db: Session):
    hourly_data = db.query(models.HourlyData).filter(models.HourlyData.po_uuid ==po_uuid).order_by(
        models.HourlyData.id.asc()).all()

    keys_data = {}
    grouped_keys = {}

    for data in hourly_data:
        if data.key not in grouped_keys:
            grouped_keys[data.key] = []
        grouped_keys[data.key].append(data)

    for key, values in grouped_keys.items():
        dynamic_key = key
        first_value = values[0].key_start
        last_value = values[-1].key_stop
        # For Length and Speed → take latest value
        if key in ["Length", "Speed"]:
            keys_data[dynamic_key] = last_value

        # For other keys → subtraction
        else:
            try:
                keys_data[dynamic_key] = last_value - first_value
            except:
                keys_data[dynamic_key] = None
        if key == "Length":
            length_values = grouped_keys.get("Length", [])

            if length_values:
                first_entry = length_values[0]
                last_entry = length_values[-1]

                start_time = first_entry.created_at
                end_time = last_entry.updated_at
                if start_time and end_time and end_time > start_time:
                    duration = (end_time - start_time).total_seconds()
                    total_length = last_entry.key_stop or 0

                    keys_data["average_speed"] = round(total_length / duration, 2) if duration > 0 else 0.0
                else:
                    keys_data["average_speed"] = 0.0

    return keys_data


@router.get("/po_details/{po_uuid}")
async def get_po_details(po_uuid: str, db: Session = Depends(get_db)):
    po_details = db.query(models.PoData).filter(models.PoData.po_uuid == po_uuid,
                                                models.PoData.stop_time != None).first()
    calculated_key = await calculate_key_value(po_uuid=po_details.po_uuid, db=db)
    return {**po_details.__dict__,"key":{**calculated_key}}


RUNNING = "RUNNING"
BREAKDOWN = "BREAKDOWN"
CHANGEOVER = "CHANGEOVER"
PLANNED_BREAK = "PLANNED_BREAK"


# =========================================================
# HELPER FUNCTIONS
# =========================================================

def merge_continuous_segments(events):
    """
    Merge same continuous status segments
    """

    if not events:
        return []

    events = sorted(events, key=lambda x: x["start_time"])

    merged = [events[0]]

    for current in events[1:]:

        last = merged[-1]

        last_end = datetime.strptime(
            last["end_time"],
            "%Y-%m-%d %H:%M:%S"
        )

        current_start = datetime.strptime(
            current["start_time"],
            "%Y-%m-%d %H:%M:%S"
        )

        # Merge same continuous status
        if (
                last["status"] == current["status"]
                and abs((current_start - last_end).total_seconds()) <= 2
        ):

            last["end_time"] = current["end_time"]

        else:
            merged.append(current)

    return merged


@router.get("/timeline/{machine_name}/{line}")
async def get_timeline(machine_name: str, line: str, date_: date, db: Session = Depends(get_db)):
    try:
        # shift_data = await get_shift_details_data(db=db)
        # today = await calculate_adjusted_date(shift_data["shift_a_start"],
        #                                       datetime.utcnow() + timedelta(hours=5, minutes=30))

        # FETCH DATA
        hourly_rows = (db.query(models.HourlyData).filter(models.HourlyData.machine_name == machine_name,
                                                          models.HourlyData.line == line,
                                                          models.HourlyData.key == 'Length',
                                                          models.HourlyData.date_ == date_).order_by(
            models.HourlyData.created_at.asc()).all())

        raw_segments = []

        # BUILD RUNNING + CHANGEOVER
        previous = None
        for row in hourly_rows:
            start_dt = row.created_at
            end_dt = row.updated_at or datetime.now()

            # RUNNING
            raw_segments.append({
                "status": RUNNING,
                "start_time": start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "end_time": end_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "po_uuid": row.po_uuid
            })

            # CHANGEOVER
            if previous:
                if previous.po_uuid != row.po_uuid:
                    previous_end = previous.updated_at
                    current_start = row.created_at
                    if (previous_end and current_start and current_start > previous_end):
                        raw_segments.append({
                            "status": CHANGEOVER,
                            "start_time": previous_end.strftime("%Y-%m-%d %H:%M:%S"),
                            "end_time": current_start.strftime("%Y-%m-%d %H:%M:%S"),
                            "from_po": previous.po_uuid,
                            "to_po": row.po_uuid
                        })

            previous = row

        raw_segments = merge_continuous_segments(raw_segments)

        # FETCH PLANNED BREAK DATA
        planned_break_data = (db.query(models.PlannedBreakData).filter(models.PlannedBreakData.line == line,
                                                                       models.PlannedBreakData.machine_name == machine_name).order_by(
            models.PlannedBreakData.id.desc()).first())

        break_intervals = []

        # PLANNED BREAK INTERVALS
        if planned_break_data:

            shift_mapping = {
                "shift_a_planned_break": "A",
                "shift_b_planned_break": "B",
                "shift_c_planned_break": "C",
                "shift_g_planned_break": "G",
            }

            for field, shift_name in shift_mapping.items():

                shift_data = getattr(planned_break_data, field, {}) or {}
                for category, values in shift_data.items():
                    if (isinstance(values, list) and len(values) == 2):
                        start_time, duration_minutes = values
                        duration_seconds = duration_minutes * 60
                        br_start = datetime.combine(date_, datetime.strptime(start_time, "%H:%M:%S").time())
                        # ------------------------------------
                        # END DATETIME
                        # -------------------------------------

                        br_end = br_start + timedelta(seconds=duration_seconds)
                        break_intervals.append({
                            "category": category,
                            "shift": shift_name,
                            "start": br_start,
                            "end": br_end,
                            "duration": duration_seconds
                        })

        # =====================================================
        # SPLIT SEGMENTS BY PLANNED BREAK
        # =====================================================

        final_items = []
        added_breaks = set()
        for item in raw_segments:
            start_dt = datetime.strptime(item["start_time"], "%Y-%m-%d %H:%M:%S")
            end_dt = datetime.strptime(item["end_time"], "%Y-%m-%d %H:%M:%S")

            current_start = start_dt
            overlap_found = False
            for br in break_intervals:

                br_start = br["start"]
                br_end = br["end"]

                if (br_end <= current_start or br_start >= end_dt):
                    continue
                overlap_found = True
                if br_start > current_start:
                    final_items.append({
                        **item,
                        "start_time": current_start.strftime("%Y-%m-%d %H:%M:%S"),
                        "end_time": br_start.strftime("%Y-%m-%d %H:%M:%S")
                    })

                # -----------------------------------------
                # PLANNED BREAK PART
                # -----------------------------------------
                break_key = (br["category"], br_start, br_end)
                if break_key not in added_breaks:
                    final_items.append({
                        "status": PLANNED_BREAK,
                        "category": br["category"],
                        "shift": br["shift"],
                        "start_time": br_start.strftime("%Y-%m-%d %H:%M:%S"),
                        "end_time": br_end.strftime("%Y-%m-%d %H:%M:%S"),
                        "duration": br["duration"]
                    })

                    added_breaks.add(break_key)

                current_start = max(current_start, br_end)
            if overlap_found:
                if current_start < end_dt:
                    final_items.append({
                        **item,
                        "start_time": current_start.strftime("%Y-%m-%d %H:%M:%S"),
                        "end_time": end_dt.strftime("%Y-%m-%d %H:%M:%S")
                    })
            else:
                final_items.append(item)

        # FETCH BREAKDOWN DATA

        breakdowns = (
            db.query(models.BreakdownData).filter(models.BreakdownData.date_ == date_,
                                                  models.BreakdownData.line == line,
                                                  models.BreakdownData.machine_name == machine_name).all())

        # APPLY BREAKDOWN OVERLAY

        updated_items = []
        for item in final_items:
            current_segments = [item]
            for bd in breakdowns:
                bd_start = bd.start_time
                bd_end = bd.stop_time or datetime.now()
                temp_segments = []
                for seg in current_segments:
                    seg_start = datetime.strptime(seg["start_time"], "%Y-%m-%d %H:%M:%S")
                    seg_end = datetime.strptime(seg["end_time"], "%Y-%m-%d %H:%M:%S")

                    if (bd_end <= seg_start or bd_start >= seg_end):
                        temp_segments.append(seg)
                        continue

                    if bd_start > seg_start:
                        temp_segments.append({
                            **seg,
                            "start_time": seg_start.strftime("%Y-%m-%d %H:%M:%S"),
                            "end_time": bd_start.strftime("%Y-%m-%d %H:%M:%S")
                        })

                    # BREAKDOWN PART
                    temp_segments.append({
                        "status": BREAKDOWN,
                        "reason": bd.reason,
                        "category": bd.category,
                        "start_time": max(seg_start, bd_start).strftime("%Y-%m-%d %H:%M:%S"),
                        "end_time": min(seg_end, bd_end).strftime("%Y-%m-%d %H:%M:%S")
                    })

                    if bd_end < seg_end:
                        temp_segments.append({
                            **seg,
                            "start_time": bd_end.strftime("%Y-%m-%d %H:%M:%S"),
                            "end_time": seg_end.strftime("%Y-%m-%d %H:%M:%S")
                        })

                current_segments = temp_segments

            updated_items.extend(current_segments)

        # SORT According to the timestamp
        updated_items = sorted(updated_items, key=lambda x: x["start_time"])

        running_duration = 0
        changeover_duration = 0
        breakdown_duration = 0
        planned_break_duration = 0

        for item in updated_items:

            start_dt = datetime.strptime(item["start_time"], "%Y-%m-%d %H:%M:%S")
            end_dt = datetime.strptime(item["end_time"], "%Y-%m-%d %H:%M:%S")
            duration = int((end_dt - start_dt).total_seconds())
            item["duration_seconds"] = duration
            if item["status"] == RUNNING:
                running_duration += duration

            elif item["status"] == CHANGEOVER:
                changeover_duration += duration

            elif item["status"] == BREAKDOWN:
                breakdown_duration += duration

            elif item["status"] == PLANNED_BREAK:
                planned_break_duration += duration
        return {
            "date": str(date_),
            "machine": machine_name,
            "line": line,
            "timeline": updated_items,
            "running_duration": running_duration,
            "changeover_duration": changeover_duration,
            "planned_break_duration": planned_break_duration,
            "breakdown_duration": breakdown_duration
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

