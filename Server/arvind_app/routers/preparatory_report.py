from collections import defaultdict
from datetime import date, datetime
from http.client import HTTPException
from fastapi import HTTPException
import pandas as pd
from http.client import HTTPException
from fastapi import HTTPException
from .. import crud, models
from ..database import SessionLocal, engine
from typing import List
from fastapi import Depends, APIRouter
from .. import Excel_Report
import logging
import sqlalchemy
import calendar
from datetime import timedelta
from sqlalchemy.orm import Session
from datetime import date
from openpyxl import load_workbook
from sqlalchemy import cast, Float, text
from datetime import date, timedelta
from sqlalchemy import cast, Float, text, func, extract

log = logging.getLogger("uvicorn")
log.setLevel(logging.INFO)
from fastapi.responses import FileResponse
import os
import sys

if getattr(sys, 'frozen', False):
    dirname = os.path.dirname(sys.executable)
else:
    dirname = os.path.dirname(os.path.abspath(__file__))


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


router = APIRouter(tags=["Preparatory Report"])


async def get_machine_production(db: Session, date_: date, machine: str):
    try:
        total_production_query = db.query(
            sqlalchemy.func.sum(models.RunData.meters).label('total_production'),
        ).filter(
            models.RunData.date_ == date_,
            models.RunData.machine == machine
        )
        total_production = total_production_query.scalar()
        return total_production if total_production else 0
    except Exception as e:
        print(e)
        return 0


async def get_production(db: Session, date_: date, machine: str, operation_name: str):
    try:
        total_production_query = db.query(
            models.RunData.machine,
            models.RunData.operation_name,
            sqlalchemy.func.sum(models.RunData.meters).label('total_production'),
        ).filter(
            models.RunData.date_ == date_,
            models.RunData.machine == machine,
            models.RunData.operation_name == operation_name
        ).group_by(models.RunData.operation_name, models.RunData.machine)
        production_by_operation = {

            result.operation_name: result.total_production or 0

            for result in total_production_query.all()
        }
        return production_by_operation
    except Exception as e:
        print(e)
        return {}


# async def get_stop_duration(db: Session, date_: date, machine: str, stop_category: str):
#    try:
#        total_duration_query = db.query(
#            models.StopData.machine,
#            models.StopData.stop_category,
#            cast(func.round(func.sum(models.StopData.duration) / 3600.0, 2), Float).label('total_duration')
#        ).filter(
#            models.StopData.date_ == date_,
#            models.StopData.machine == machine,
#            models.StopData.stop_category == stop_category
#        ).group_by(models.StopData.stop_category, models.StopData.machine)
#
#        stop_duration_by_category = {
#            result.stop_category: result.total_duration or 0
#            for result in total_duration_query.all()
#        }
#        return stop_duration_by_category
#    except Exception as e:
#        print(e)
#        return {}


async def get_stop_duration(db: Session, date_: date, machine: str, stop_category: str):
    try:
        total_duration_query = db.query(
            models.StopData.machine,
            models.StopData.stop_category,
            func.sum(models.StopData.duration).label('total_duration')
        ).filter(
            models.StopData.date_ == date_,
            models.StopData.machine == machine,
            models.StopData.stop_category == stop_category
        ).group_by(models.StopData.stop_category, models.StopData.machine)

        # Fetch the results first
        stop_duration_by_category = {
            result.stop_category: round(result.total_duration / 3600.0, 2) if result.total_duration else 0
            for result in total_duration_query.all()
        }

        return stop_duration_by_category
    except Exception as e:
        print(e)
        return {}


async def get_machine_duration(db: Session, date_: date, machine: str):
    try:
        total_production_query = db.query(
            cast(sqlalchemy.func.sum(models.RunData.duration) / 3600.0, Float).label('production_hours'),
        ).filter(
            models.RunData.date_ == date_,
            models.RunData.machine == machine
        )
        total_production = total_production_query.scalar()
        return total_production if total_production else 0
    except Exception as e:
        print(e)
        return 0


def get_month_date_range(date_: date):
    year = date_.year
    month = date_.month
    num_days = calendar.monthrange(year, month)[1]
    start_date = date(year, month, 1)
    end_date = date(year, month, num_days)
    return start_date, end_date


async def fetch_machine_production_for_month(db: Session, date_: date, machine: str):
    start_date, end_date = get_month_date_range(date_)
    month_production = {}
    current_date = start_date
    while current_date <= end_date:
        daily_production = await get_machine_production(db, current_date, machine)
        # formatted_date = current_date.strftime("%d-%B-%Y")
        formatted_date = current_date.strftime("%d-%b-%Y")
        month_production[formatted_date] = round(daily_production, 0)
        current_date += timedelta(days=1)
    return month_production


async def fetch_machine_duration_for_month(db: Session, date_: date, machine: str):
    start_date, end_date = get_month_date_range(date_)
    month_production = {}
    current_date = start_date
    while current_date <= end_date:
        daily_production = await get_machine_duration(db, current_date, machine)
        # formatted_date = current_date.strftime("%d-%B-%Y")
        formatted_date = current_date.strftime("%d-%b-%Y")
        month_production[formatted_date] = round(daily_production, 0)
        current_date += timedelta(days=1)
    return month_production


async def fetch_operation_production_for_month(db: Session, date_: date, machine: str, operation_name: str):
    start_date, end_date = get_month_date_range(date_)
    month_production = {}
    current_date = start_date
    while current_date <= end_date:
        daily_production = await get_production(db, current_date, machine, operation_name)
        # formatted_date = current_date.strftime("%d-%B-%Y")
        formatted_date = current_date.strftime("%d-%b-%Y")
        month_production[formatted_date] = daily_production
        current_date += timedelta(days=1)
    return month_production


async def fetch_operation_duration_for_month(db: Session, date_: date, machine: str, stop_category: str):
    start_date, end_date = get_month_date_range(date_)
    month_production = {}
    current_date = start_date
    while current_date <= end_date:
        daily_production = await get_stop_duration(db, current_date, machine, stop_category)
        # formatted_date = current_date.strftime("%d-%B-%Y")
        formatted_date = current_date.strftime("%d-%b-%Y")
        month_production[formatted_date] = daily_production
        current_date += timedelta(days=1)
    return month_production


async def fetch_machine_production_till_date(db: Session, date_: date, machine: str):
    start_date, end_date = get_month_date_range(date_)
    month_production = {}
    current_date = start_date
    cumulative_production = 0  # Initialize cumulative production

    while current_date <= end_date:
        daily_production = await get_machine_production(db, current_date, machine)
        cumulative_production += daily_production  # Accumulate daily production

        formatted_date = current_date.strftime("%d-%b-%Y")
        month_production[formatted_date] = round(cumulative_production, 0)  # Store cumulative production

        current_date += timedelta(days=1)

    return month_production


async def fetch_machine_duration_till_date(db: Session, date_: date, machine: str):
    start_date, end_date = get_month_date_range(date_)
    month_production = {}
    current_date = start_date
    cumulative_production = 0  # Initialize cumulative production

    while current_date <= end_date:
        daily_production = await get_machine_duration(db, current_date, machine)
        cumulative_production += daily_production  # Accumulate daily production

        formatted_date = current_date.strftime("%d-%b-%Y")
        month_production[formatted_date] = round(cumulative_production, 0)  # Store cumulative production

        current_date += timedelta(days=1)

    return month_production


async def calculate_production_in_kg(db: Session, date_: date, machine: str):
    try:
        month = date_.month
        current_year = date_.year
        first_day_of_month = datetime.date(datetime(current_year, month, 1))
        last_day_of_month = first_day_of_month.replace(day=calendar.monthrange(current_year, month)[1])
        date_range = pd.date_range(first_day_of_month, last_day_of_month, freq='D')

        production_dict = {}
        for date_data in date_range:
            unique_po_numbers = db.query(
                models.RunData.machine,
                models.RunData.po_number
            ).filter(
                models.RunData.date_ == date_data,
                models.RunData.machine == machine
            ).group_by(models.RunData.machine, models.RunData.po_number)

            po_data_query = unique_po_numbers.all()
            po_machines = [item[0] for item in po_data_query]
            unique_po_numbers = [item[1] for item in po_data_query if
                                 item[1] != 'No PO Number']  # Exclude 'No PO Number'
            unique_po_count = len(set(unique_po_numbers))

            aggregated_glm_data = {machine_name: 0 for machine_name in set(po_machines)}

            for machine, po_number in zip(po_machines, unique_po_numbers):
                glm_query = db.query(
                    models.PoData.machine,
                    func.sum(models.PoData.greige_glm).label('greige_glm_sum')
                ).filter(
                    models.PoData.machine == machine,
                    models.PoData.po_number == po_number
                ).group_by(models.PoData.machine)

                glm_query_data = glm_query.first()

                if glm_query_data:
                    machine_name, greige_glm_sum = glm_query_data
                    aggregated_glm_data[machine_name] += greige_glm_sum

            total_greige_glm_sum = sum(aggregated_glm_data.values())

            # Calculate the average greige_glm
            avg_glm_per_date = total_greige_glm_sum / unique_po_count if unique_po_count != 0 else 0
            ## calculate total production
            #            total_production_per_date = await get_machine_production(db, date_data, machine)
            #            production_kg = int(avg_glm_per_date) * int(total_production_per_date)
            total_production_per_date = await get_machine_production(db, date_data, machine)
            production_kg = int(total_production_per_date) * 0.241
            #            production_dict[date_data.strftime('%Y-%m-%d')] = int(production_kg)
            production_dict[date_data.strftime('%d-%b-%Y')] = int(production_kg)

        return production_dict
    except Exception as e:
        print(e)


##################### GET THE TILL DATE KG PRODUCTION
async def calculate_production_in_kg_till_date(db: Session, date_: date, machine: str):
    try:
        date_range = get_monthly_date_range(date_)

        production_dict_data = {}
        cumulative_production_data = 0  # Initialize cumulative production

        for date_data in date_range:
            unique_po_numbers = db.query(
                models.RunData.machine,
                models.RunData.po_number
            ).filter(
                models.RunData.date_ == date_data,
                models.RunData.machine == machine
            ).group_by(models.RunData.machine, models.RunData.po_number)

            po_data_query = unique_po_numbers.all()
            po_machines = [item[0] for item in po_data_query]
            unique_po_numbers = [item[1] for item in po_data_query if
                                 item[1] != 'No PO Number']  # Exclude 'No PO Number'
            unique_po_count = len(set(unique_po_numbers))

            aggregated_glm_data = {machine_name: 0 for machine_name in set(po_machines)}

            for machine, po_number in zip(po_machines, unique_po_numbers):
                glm_query = db.query(
                    models.PoData.machine,
                    func.sum(models.PoData.greige_glm).label('greige_glm_sum')
                ).filter(
                    models.PoData.machine == machine,
                    models.PoData.po_number == po_number
                ).group_by(models.PoData.machine)

                glm_query_data = glm_query.first()

                if glm_query_data:
                    machine_name, greige_glm_sum = glm_query_data
                    aggregated_glm_data[machine_name] += greige_glm_sum

            total_greige_glm_sum = sum(aggregated_glm_data.values())

            # Calculate the average greige_glm
            avg_glm_per_date = total_greige_glm_sum / unique_po_count if unique_po_count != 0 else 0
            total_production_per_date = await get_machine_production(db, date_data, machine)
            production_kg = int(avg_glm_per_date) * int(total_production_per_date)

            # Update cumulative production
            cumulative_production_data += int(total_production_per_date)
            #            production_dict_data[date_data.strftime('%Y-%m-%d')] = cumulative_production_data
            production_dict_data[date_data.strftime('%d-%b-%Y')] = cumulative_production_data

        return production_dict_data
    except Exception as e:
        print(e)


async def get_total_stop_duration_excluding_categories(db: Session, date_: date, machine: str,
                                                       excluded_stop_categories: list):
    try:
        # Query to sum the duration of all stop categories except the ones in the excluded list
        total_duration = db.query(
            func.sum(models.StopData.duration).label('total_duration')  # Summing durations (raw value)
        ).filter(
            models.StopData.date_ == date_,
            models.StopData.machine == machine,
            models.StopData.stop_category.notin_(excluded_stop_categories)  # Exclude the list of stop categories
        ).scalar()  # Fetch a single scalar value (total sum)

        # Apply division and rounding after fetching the result
        total_duration_in_hours = round(total_duration / 3600.0, 2) if total_duration else 0

        # Return a dictionary with the date as the key and total duration as the value
        return {str(date_): total_duration_in_hours}
    except Exception as e:
        print(e)
        return {str(date_): 0}


async def fetch_misc_duration_till_month(db: Session, date_: date, machine: str, stop_list: list):
    start_date, end_date = get_month_date_range(date_)
    month_production = {}
    current_date = start_date
    cumulative_production = 0  # Initialize cumulative production

    while current_date <= end_date:
        # Fetch the daily production, which is a dict in the form {date_: sum_duration}
        daily_production_dict = await get_total_stop_duration_excluding_categories(db, current_date, machine, stop_list)

        # Extract the sum_duration from the dictionary
        daily_production = list(daily_production_dict.values())[0]

        print(daily_production)
        print(cumulative_production)

        # Accumulate daily production
        cumulative_production += daily_production

        # Format the date and store cumulative production
        formatted_date = current_date.strftime("%d-%b-%Y")
        month_production[formatted_date] = round(cumulative_production, 0)

        # Move to the next day
        current_date += timedelta(days=1)

    return month_production


STENTER_LIST = ["Stenter-1", "Stenter-2", "Stenter-3", "Stenter-4"]
excluded_sc = ['Machine cleaning', 'Lead cloth burst']


# @router.get("/calculate_production_in_kg11/{date_}/", )
# async def calculate_production_in_kg11(date_: date, db: Session = Depends(get_db)):
#    return await fetch_misc_duration_till_month(db=db, date_=date_, machine="Stenter-2",
#                                                stop_list=excluded_sc)

@router.get("/calculate_production_in_kg_data/{date_}/{machine}", )
async def calculate_production_in_kg_data(date_: date, machine: str, db: Session = Depends(get_db)):
    return await calculate_production_in_kg(db=db, date_=date_, machine=machine)


@router.get("/calculate_production_in_kg_till_date_data/{date_}/{machine}", )
async def calculate_production_in_kg_till_date_data(date_: date, machine: str, db: Session = Depends(get_db)):
    return await calculate_production_in_kg_till_date(db=db, date_=date_, machine=machine)


@router.get("/fetch_operation_production_for_month_data/{date_}/{machine}/{operation_name}", )
async def fetch_operation_production_for_month_data(date_: date, machine: str, operation_name: str,
                                                    db: Session = Depends(get_db)):
    return await fetch_operation_production_for_month(db=db, date_=date_, machine=machine,
                                                      operation_name=operation_name)


@router.get("/fetch_machine_production_for_month_data/{date_}/{machine}", )
async def fetch_machine_production_for_month_data(date_: date, machine: str, db: Session = Depends(get_db)):
    return await fetch_machine_production_for_month(db=db, date_=date_, machine=machine)


####    ................................ PREPARATORY PRODUCTION REPORT API'S  .........................................

####    ................................ PREPARATORY PRODUCTION REPORT API'S  .........................................
def get_monthly_date_range(date_: date):
    month = date_.month
    current_year = date_.year
    first_day_of_month = datetime.date(datetime(current_year, month, 1))
    last_day_of_month = first_day_of_month.replace(day=calendar.monthrange(current_year, month)[1])
    date_from = first_day_of_month.strftime("%Y-%m-%d")
    date_to = last_day_of_month.strftime("%Y-%m-%d")
    date_range = pd.date_range(date_from, date_to, freq='D')
    return date_range


async def get_monthly_machines_production(db: Session, date_: date):
    try:
        date_range = get_monthly_date_range(date_)
        all_machines = ['SANDO', 'Osthoff-1', 'Osthoff-2', 'PTR', 'Perble', 'Merceriser-1', 'Merceriser-2', 'NPS',
                        'Batcher-1', 'Batcher-2',
                        'OSTHOFF-1 BATCH', 'Xetma-2', 'Xetma-1', 'Lafer-1', 'Lafer-2', 'Lafer-3',
                        'Lafer-4', 'Lafer-5-Raising', 'Lafer-5-Shearing', 'Soaper']
        production_dict = {}
        present_date = datetime.today().date()
        for date_data in date_range:

            query = db.query(
                models.RunData.machine,
                sqlalchemy.func.sum(models.RunData.meters).label('production')
            ).filter(
                models.RunData.date_ == date_data
            ).group_by(models.RunData.machine)

            data = query.all()
            for machine in all_machines:
                if data:

                    production_value = next((production for m, production in data if m == machine), 0)
                else:
                    production_value = 0

                if date_data.date() == present_date:
                    production_value = 0

                data_production = {
                    "date": date_data.strftime('%Y-%m-%d'),
                    "machine": machine,
                    "production": int(production_value)
                }

                if machine not in production_dict:
                    production_dict[machine] = []

                production_dict[machine].append(data_production)

        return production_dict

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{e}")


async def get_production_kg_of_each_machine(db: Session, date_: date):
    try:
        all_machines = ['Stenter-2', 'Osthoff-1', 'Osthoff-2', 'PTR', 'Perble', 'Merceriser-1', 'Merceriser-2', 'NPS',
                        'Batcher-1', 'Batcher-2', 'OSTHOFF-1 BATCH',
                        'Xetma-2', 'Xetma-1', 'Lafer-1', 'Lafer-2', 'Lafer-3', 'Lafer-4', 'Lafer-5-Raising',
                        'Lafer-5-Shearing', 'Soaper']

        production_results = []
        for machine in all_machines:
            machine_production_in_kg = await calculate_production_in_kg(db, date_, machine)
            production_results.append({machine: machine_production_in_kg})
        return production_results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{e}")


async def get_reprocess_production_data(db: Session, date_: date):
    date_range = get_monthly_date_range(date_)
    all_machines = ['PTR', 'NPR', 'Merceriser-1', 'Merceriser-2']
    production_results = {}
    for machine in all_machines:
        production_results[machine] = {}
        for date_data in date_range:
            reprocess_production = await get_production(db, date_data, machine, "Reprocess")
            production_results[machine][date_data.strftime('%Y-%m-%d')] = reprocess_production.get("Reprocess", 0)
    return production_results


async def get_monthly_effective_production(db: Session, date_: date):
    try:
        date_range = get_monthly_date_range(date_)
        all_machines = ['SANDO', 'Osthoff-1', 'Osthoff-2', 'PTR', 'Perble', 'Merceriser-1', 'Merceriser-2', 'NPS',
                        'Batcher-1', 'Batcher-2',
                        'Osthoff-1 BATCH', 'Xetma-2', 'Xetma-1', 'Lafer-1', 'Lafer-2', 'Lafer-3', 'Lafer-4',
                        'Lafer-5-Raising', 'Lafer-5-Shearing',
                        'Soaper']
        production_dict = {}
        present_date = datetime.today().date()
        for date_data in date_range:

            query = db.query(
                models.RunData.machine,
                sqlalchemy.func.sum(models.RunData.meters).label('production')
            ).filter(
                models.RunData.date_ == date_data,
                models.RunData.operation_name != "Reprocess"
            ).group_by(models.RunData.machine)

            data = query.all()
            for machine in all_machines:
                if data:

                    production_value = next((production for m, production in data if m == machine), 0)
                else:
                    production_value = 0

                if date_data.date() == present_date:
                    production_value = 0

                data_production = {
                    "date": date_data.strftime('%Y-%m-%d'),
                    "machine": machine,
                    "production": int(production_value)
                }

                if machine not in production_dict:
                    production_dict[machine] = []

                production_dict[machine].append(data_production)

        return production_dict

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{e}")


async def get_machine_duration(db: Session, date_: date, machine: str):
    try:
        total_production_query = db.query(
            cast(sqlalchemy.func.sum(models.RunData.duration) / 3600.0, Float).label('production_hours'),
        ).filter(
            models.RunData.date_ == date_,
            models.RunData.machine == machine
        )
        total_production = total_production_query.scalar()
        return total_production if total_production else 0
    except Exception as e:
        print(e)
        return 0


async def get_daily_production_shift_wise(db: Session, date_: date):
    try:
        machine_list = ['STG', 'Osthoff-1', 'Osthoff-2', 'PTR', 'Perble', 'Merceriser-1', 'Merceriser-2', 'NPS',
                        'Batcher-1',
                        'Batcher-2', 'SINGEING -1 BATCH', 'Xetma-2', 'Xetma-3', 'Lafer-1', 'Lafer-2', 'Lafer-3',
                        'Lafer-4',
                        'Lafer-5-Raising', 'Lafer-5-Shearing', 'Soaper']
        results = []
        for machine in machine_list:
            data = db.query(
                models.RunData.machine,
                models.RunData.shift,
                sqlalchemy.func.sum(models.RunData.meters).label('production')
            ).filter(
                models.RunData.date_ == date_,
                models.RunData.machine == machine
            ).group_by(models.RunData.machine, models.RunData.shift).order_by(
                models.RunData.shift
            ).all()
            reprocess_data = await get_production(db, date_, machine, "Reprocess")
            process_demand_data = await get_production(db, date_, machine, "Process demand")
            running_hours = await get_machine_duration(db, date_, machine)

            machine_result = {
                "machine": machine,
                **{record.shift: round(record.production, 2) for record in data},
                "reprocess_data": reprocess_data.get("Reprocess", 0),
                "process_demand_data": process_demand_data.get("Process demand", 0),
                "running_hours": running_hours

            }
            results.append(machine_result)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{e}")


async def get_shift_wise_glm_production(db: Session, date_: date, machine: str, operation_name: str = None):
    try:
        query = db.query(
            models.RunData.machine,
            models.RunData.po_number,
            models.RunData.shift
        ).filter(
            models.RunData.date_ == date_,
            models.RunData.machine == machine
        )
        if operation_name:
            query = query.filter(models.RunData.operation_name == operation_name)

        unique_po_numbers = query.group_by(models.RunData.machine, models.RunData.po_number, models.RunData.shift)
        po_data_query = unique_po_numbers.all()
        aggregated_glm_data = defaultdict(lambda: defaultdict(int))
        for machine, po_number, shift in po_data_query:
            glm_query = db.query(
                models.PoData.machine,
                func.sum(models.PoData.greige_glm).label('greige_glm_sum')
            ).filter(
                models.PoData.machine == machine,
                models.PoData.po_number == po_number
            ).group_by(models.PoData.machine)

            glm_query_data = glm_query.first()

            if glm_query_data:
                machine_name, greige_glm_sum = glm_query_data
                aggregated_glm_data[shift][machine_name] += greige_glm_sum

        total_greige_glm_sum_shift_wise = {shift: 0 for shift in ['A', 'B', 'C']}  # Adjust shifts as needed

        for shift, data in aggregated_glm_data.items():
            # total_greige_glm_sum_shift_wise[shift] = sum(data.values())
            total_greige_glm_sum_shift_wise[shift] = round(sum(data.values()), 2)

        return total_greige_glm_sum_shift_wise

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{e}")


async def get_effective_production_data(db: Session, date_: date, machine_list: list):
    try:
        results = []
        all_shifts = ['A', 'B', 'C']

        for machine in machine_list:
            data = db.query(
                models.RunData.machine,
                models.RunData.shift,
                sqlalchemy.func.sum(models.RunData.meters).label('production')
            ).filter(
                models.RunData.date_ == date_,
                models.RunData.machine == machine,
                models.RunData.operation_name != "Reprocess"  # Exclude "Reprocess"
            ).group_by(models.RunData.machine, models.RunData.shift).order_by(
                models.RunData.shift
            ).all()
            production_data = {shift: 0 for shift in all_shifts}
            glm_data = await get_shift_wise_glm_production(db, date_, machine)
            for record in data:
                production_data[record.shift] = round(record.production, 2)
            machine_result = {
                "machine": machine,
                "production_data": production_data,
                "glm_data": glm_data
            }
            results.append(machine_result)

        return results

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{e}")


async def get_reprocess_production_data_by_machine_list(db: Session, date_: date, machine_list: list):
    try:
        results = []
        all_shifts = ['A', 'B', 'C']

        for machine in machine_list:
            data = db.query(
                models.RunData.machine,
                models.RunData.shift,
                sqlalchemy.func.sum(models.RunData.meters).label('production')
            ).filter(
                models.RunData.date_ == date_,
                models.RunData.machine == machine,
                models.RunData.operation_name == "Reprocess"  # Filter for "Reprocess"
            ).group_by(models.RunData.machine, models.RunData.shift).order_by(
                models.RunData.shift
            ).all()
            glm_data = await get_shift_wise_glm_production(db, date_, machine, "Reprocess")
            production_data = {shift: 0 for shift in all_shifts}
            for record in data:
                production_data[record.shift] = round(record.production, 2)
            machine_result = {
                "machine": machine,
                "production_data": production_data,
                "glm_data": glm_data
            }
            results.append(machine_result)

        return results

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{e}")


@router.get("/get_glm_production_data/{date_}/{machine}", )
async def get_glm_production_data(date_: date, machine: str, db: Session = Depends(get_db)):
    return await get_shift_wise_glm_production(db=db, date_=date_, machine=machine)


async def get_per_kg_data(db: Session, date_: date, machine: str):
    production_dict_data = {}
    cumulative_production_data = 0
    unique_po_numbers = db.query(
        models.RunData.machine,
        models.RunData.po_number
    ).filter(
        models.RunData.date_ == date_,
        models.RunData.machine == machine
    ).group_by(models.RunData.machine, models.RunData.po_number)

    po_data_query = unique_po_numbers.all()
    po_machines = [item[0] for item in po_data_query]
    unique_po_numbers = [item[1] for item in po_data_query if
                         item[1] != 'No PO Number']  # Exclude 'No PO Number'
    unique_po_count = len(set(unique_po_numbers))

    aggregated_glm_data = {machine_name: 0 for machine_name in set(po_machines)}

    for machine, po_number in zip(po_machines, unique_po_numbers):
        glm_query = db.query(
            models.PoData.machine,
            func.sum(models.PoData.greige_glm).label('greige_glm_sum')
        ).filter(
            models.PoData.machine == machine,
            models.PoData.po_number == po_number
        ).group_by(models.PoData.machine)

        glm_query_data = glm_query.first()

        if glm_query_data:
            machine_name, greige_glm_sum = glm_query_data
            aggregated_glm_data[machine_name] += greige_glm_sum

    total_greige_glm_sum = sum(aggregated_glm_data.values())

    # Calculate the average greige_glm
    avg_glm_per_date = total_greige_glm_sum / unique_po_count if unique_po_count != 0 else 0
    total_production_per_date = await get_machine_production(db, date_, machine)
    production_kg = int(avg_glm_per_date) * int(total_production_per_date)

    # Update cumulative production
    cumulative_production_data += int(production_kg)
    production_dict_data[date_.strftime('%d-%b-%Y')] = cumulative_production_data

    return cumulative_production_data


async def get_per_kg_data_for_month(db: Session, date_: date, machine: str):
    start_date, end_date = get_month_date_range(date_)
    month_production = {}
    current_date = start_date
    while current_date <= end_date:
        daily_production = await get_per_kg_data(db, current_date, machine)
        # formatted_date = current_date.strftime("%d-%B-%Y")
        formatted_date = current_date.strftime("%d-%b-%Y")
        month_production[formatted_date] = round(daily_production, 0)
        current_date += timedelta(days=1)
    return month_production


@router.get("/get_per_kg_data111/{date_}/", )
async def get_per_kg_data111(date_: date, db: Session = Depends(get_db)):
    return await get_per_kg_data_for_month(db=db, date_=date_, machine="Stenter-2")


async def get_machine_production_prod_kgs(db: Session, date_: date, machine: str):
    try:
        total_production_query = db.query(
            sqlalchemy.func.sum(models.RunData.meters).label('total_production'),
        ).filter(
            models.RunData.date_ == date_,
            models.RunData.machine == machine
        )
        total_production = total_production_query.scalar()

        # Multiply the total production by 0.241
        return (total_production * 0.241) if total_production else 0
    except Exception as e:
        print(e)
        return 0


async def fetch_prod_kgs_machine_production_for_month(db: Session, date_: date, machine: str):
    start_date, end_date = get_month_date_range(date_)
    month_production = {}
    current_date = start_date
    while current_date <= end_date:
        daily_production = await get_machine_production_prod_kgs(db, current_date, machine)
        # formatted_date = current_date.strftime("%d-%B-%Y")
        formatted_date = current_date.strftime("%d-%b-%Y")
        month_production[formatted_date] = round(daily_production, 0)
        current_date += timedelta(days=1)
    return month_production


async def fetch_mtd_prod_kgs_machine_production_till_date(db: Session, date_: date, machine: str):
    start_date, end_date = get_month_date_range(date_)
    month_production = {}
    current_date = start_date
    cumulative_production = 0  # Initialize cumulative production

    while current_date <= end_date:
        daily_production = await get_machine_production_prod_kgs(db, current_date, machine)
        print(daily_production)
        print(cumulative_production)
        cumulative_production += daily_production  # Accumulate daily production

        formatted_date = current_date.strftime("%d-%b-%Y")
        month_production[formatted_date] = round(cumulative_production, 0)  # Store cumulative production

        current_date += timedelta(days=1)


async def fetch_misc_duration_daily(db: Session, date_: date, machine: str, stop_list: list):
    start_date, end_date = get_month_date_range(date_)  # Get the start and end dates of the month
    month_production = {}
    current_date = start_date

    while current_date <= end_date:
        # Fetch daily stop duration excluding specified categories
        daily_production_dict = await get_total_stop_duration_excluding_categories(db, current_date, machine, stop_list)
        daily_production = list(daily_production_dict.values())[0]

        # Store the daily production in the dictionary
        month_production[current_date.strftime("%d-%b-%Y")] = round(daily_production, 2)

        current_date += timedelta(days=1)

    return month_production

# @router.get("/get_daily_production_shift_wise_data/{date_}/{machine}", )
# async def get_daily_production_shift_wise_data(date_: date, db: Session = Depends(get_db)):
#    return await get_daily_production_shift_wise(db=db, date_=date_)


# async def get_production_in_kg_till_date_of_each_machine(db: Session, date_: date):
#    try:
#        all_machines = ['Stenter-2', 'OSTHOFF-1', 'OSTHOFF-2', 'PTR', 'NPR', 'OMR', 'NMR', 'NPS', 'Batcher-1',
#                        'Batcher-2',
#                        'OSTHOFF-1 BATCH',
#                        'X2', 'X3', 'LAFFER', 'LAFFER-2', 'LAFFER-3', 'LAFFER-4', 'LAFFER RASING', 'LAFFER SHARING',
#                        'SOAPER-2']
#        production_results = []
#        for machine in all_machines:
#            machine_production_in_kg_till_date = await calculate_production_in_kg_till_date(db, date_, machine)
#            production_results.append({machine: machine_production_in_kg_till_date})
#        return production_results
#    except Exception as e:
#        print(e)
#
#
# @router.get("/get_machine_production11/{date_}/", )
# async def get_machine_production11(date_: date, db: Session = Depends(get_db)):
#    return await calculate_production_in_kg(db=db, date_=date_, machine="OSTHOFF-2")
