from typing import List, Dict, Any
from datetime import date, datetime

import pandas as pd

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


router = APIRouter(tags=["Dyeing Report"])


async def get_machine_production(db: Session, date_: date, machine_list: List[str]) -> int:
    try:
        total_production_query = db.query(
            sqlalchemy.func.sum(models.RunData.meters).label('total_production'),
        ).filter(
            models.RunData.date_ == date_,
            models.RunData.machine.in_(machine_list)
        )
        total_production = total_production_query.scalar()
        return total_production if total_production else 0
    except Exception as e:
        print(e)
        return 0


async def get_machine_duration(db: Session, date_: date, machine_list: List[str]) -> int:
    try:
        total_production_query = db.query(
            cast(sqlalchemy.func.sum(models.RunData.duration) / 3600.0, Float).label('production_hours'),
        ).filter(
            models.RunData.date_ == date_,
            models.RunData.machine.in_(machine_list)
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


async def get_machine_production_for_month(db: Session, date_: date, machine_list: List[str]):
    start_date, end_date = get_month_date_range(date_)
    month_production = {}
    current_date = start_date
    while current_date <= end_date:
        daily_production = await get_machine_production(db, current_date, machine_list)
        # formatted_date = current_date.strftime("%d-%B-%Y")
        formatted_date = current_date.strftime("%d-%b-%Y")
        month_production[formatted_date] = round(daily_production, 0)
        current_date += timedelta(days=1)
    return month_production


##...............................Pad Steamer Prod.....................................

# async def get_machine_op_production(db: Session, date_: date, machine: str):
#     try:
#         total_production_query = db.query(
#             models.RunData.operation_name,
#             sqlalchemy.func.sum(models.RunData.meters).label('total_production'),
#         ).filter(
#             models.RunData.date_ == date_,
#             models.RunData.machine == machine,
#             models.RunData.operation_name.in_(['VATDEV', 'CPB', 'ECOWASH'])
#         ).group_by(models.RunData.operation_name)
#         production_by_operation = {
#             result.operation_name: int(result.total_production or 0)
#             for result in total_production_query.all()
#         }
#         return production_by_operation
#     except Exception as e:
#         print(e)
#         return {}

async def get_machine_op_production(db: Session, date_: date, machine: str):
    try:
        # Define operation groups
        operation_mapping = {
            'VATDEV': ['DYEDEVLP'],
            'CPB': ['CPBSOAP'],
            'ECOWASH': ['ECOSOAP']
        }

        # Flatten all operation names to use in the filter
        all_operations = [op for ops in operation_mapping.values() for op in ops]

        # Query all relevant operations
        total_production_query = db.query(
            models.RunData.operation_name,
            sqlalchemy.func.sum(models.RunData.meters).label('total_production'),
        ).filter(
            models.RunData.date_ == date_,
            models.RunData.machine == machine,
            models.RunData.operation_name.in_(all_operations)
        ).group_by(models.RunData.operation_name)

        # Initialize production summary
        production_by_operation = {'VATDEV': 0, 'CPB': 0, 'ECOWASH': 0}

        # Accumulate totals according to mapping
        for result in total_production_query.all():
            for group_name, aliases in operation_mapping.items():
                if result.operation_name in aliases:
                    production_by_operation[group_name] += int(result.total_production or 0)
                    break

        return production_by_operation
    except Exception as e:
        print(e)
        return {}


async def get_machine_op_production_for_month(db: Session, date_: date, machine: str):
    start_date = date_.replace(day=1)
    end_date = date_
    month_production = {}
    current_date = start_date
    while current_date <= end_date:
        daily_production = await get_machine_op_production(db, current_date, machine)
        formatted_date = current_date.strftime("%d-%b-%Y")
        month_production[formatted_date] = daily_production
        current_date += timedelta(days=1)

    return month_production


#
#
# async def get_production(db: Session, date_: date, machine: str, operation_name: str):
#     try:
#         total_production_query = db.query(
#             models.RunData.machine,
#             models.RunData.operation_name,
#             sqlalchemy.func.sum(models.RunData.meters).label('total_production'),
#         ).filter(
#             models.RunData.date_ == date_,
#             models.RunData.machine == machine,
#             models.RunData.operation_name == operation_name
#         ).group_by(models.RunData.operation_name, models.RunData.machine)
#         production_by_operation = {
#
#             result.operation_name: int(result.total_production or 0)
#
#             for result in total_production_query.all()
#         }
#         return production_by_operation
#     except Exception as e:
#         print(e)
#         return {}
#
#
# async def get_production_till_date(db: Session, date_: date, machine: str, operation_name: str):
#     # Set start_date to the 1st day of the given month
#     start_date = date_.replace(day=1)
#     end_date = date_
#     month_production = {}
#     current_date = start_date
#     while current_date <= end_date:
#         daily_production = await get_production(db, current_date, machine, operation_name)
#         formatted_date = current_date.strftime("%d-%b-%Y")
#         month_production[formatted_date] = daily_production
#         current_date += timedelta(days=1)
#
#     return month_production

async def get_production(db: Session, date_: date, machine: str, operation_names: list[str]):
    try:
        total_production_query = db.query(
            models.RunData.machine,
            models.RunData.operation_name,
            sqlalchemy.func.sum(models.RunData.meters).label('total_production'),
        ).filter(
            models.RunData.date_ == date_,
            models.RunData.machine == machine,
            models.RunData.operation_name.in_(operation_names)
        ).group_by(models.RunData.operation_name, models.RunData.machine)

        total = sum(int(result.total_production or 0) for result in total_production_query.all())
        return {operation_names[0]: total}  # group under the first operation name
    except Exception as e:
        print(e)
        return {operation_names[0]: 0}


async def get_production_till_date(db: Session, date_: date, machine: str, operation_name: str | list[str]):
    start_date = date_.replace(day=1)
    end_date = date_
    month_production = {}
    current_date = start_date
    while current_date <= end_date:
        # Ensure we're passing a list
        op_names = [operation_name] if isinstance(operation_name, str) else operation_name
        daily_production = await get_production(db, current_date, machine, op_names)
        formatted_date = current_date.strftime("%d-%b-%Y")
        month_production[formatted_date] = daily_production
        current_date += timedelta(days=1)

    return month_production


async def get_machine_production_till_date(db: Session, date_: date, machine: List[str]):
    # Set start_date to the 1st day of the given month
    start_date = date_.replace(day=1)
    end_date = date_
    month_production = {}
    current_date = start_date
    while current_date <= end_date:
        daily_production = await get_machine_production(db, current_date, machine)
        formatted_date = current_date.strftime("%d-%b-%Y")
        month_production[formatted_date] = round(daily_production, 0)
        current_date += timedelta(days=1)
    return month_production


async def get_machine_duration_till_date(db: Session, date_: date, machine: List[str]):
    # Set start_date to the 1st day of the given month
    start_date = date_.replace(day=1)
    end_date = date_
    month_production = {}
    current_date = start_date
    while current_date <= end_date:
        daily_production = await get_machine_duration(db, current_date, machine)
        formatted_date = current_date.strftime("%d-%b-%Y")
        month_production[formatted_date] = round(daily_production, 0)
        current_date += timedelta(days=1)
    return month_production


MACHINE_CODE_MAP = {
    "Jigger-1": "FJS00101",
    "Jigger-2": "FJS00102",
    "Jigger-3": "FJS00103",
    "Sclavos-1": "FBD00501",
    "Sclavos-2": "FBD00502",
    "Sclavos-3": "FBD00503",
    "Sclavos-4": "FBD00504"
}


def get_month_start_end_from_date(date_: date) -> (date, date):
    start_date = date(date_.year, date_.month, 1)
    if date_.month == 12:
        end_date = date(date_.year + 1, 1, 1) - timedelta(days=1)
    else:
        end_date = date(date_.year, date_.month + 1, 1) - timedelta(days=1)
    return start_date, end_date


async def get_jet_production(db: Session, date_: date, machine_list: List[str]) -> List[Dict[str, Any]]:
    try:
        start_date, end_date = get_month_start_end_from_date(date_)
        total_production_query = db.query(
            models.RunData.date_,
            models.RunData.operation_name,
            models.RunData.machine,
            models.RunData.po_number,
            sqlalchemy.func.sum(models.RunData.meters).label('total_meters'),
            cast(sqlalchemy.func.sum(models.RunData.duration) / 3600.0, Float).label('total_hours')
        ).filter(
            models.RunData.date_.between(start_date, end_date),
            models.RunData.machine.in_(machine_list),
            models.RunData.duration > 0.0028
        ).group_by(
            models.RunData.date_,
            models.RunData.machine,
            models.RunData.operation_name,
            models.RunData.po_number
        ).order_by(
            models.RunData.date_.asc()
        ).all()
        result_dict = []
        for result in total_production_query:
            machine = result.machine
            po_number = result.po_number
            po_data_query = db.query(
                models.PoData.machine,
                models.PoData.po_number,
                models.PoData.article,
                models.PoData.finish_glm,
                models.PoData.greige_glm
            ).filter(
                models.PoData.machine == machine,
                models.PoData.po_number == po_number
            ).first()
            data = {
                "machine": MACHINE_CODE_MAP.get(machine, machine),
                "po_number": po_data_query.po_number if po_data_query else po_number,
                "article": po_data_query.article if po_data_query else None,
                "finish_glm": po_data_query.finish_glm if po_data_query else 0,
                "greige_glm": po_data_query.greige_glm if po_data_query else 0
            }
            result_dict.append({
                "date": result.date_,
                "operation_name": result.operation_name,
                "machine": MACHINE_CODE_MAP.get(result.machine, result.machine),
                "po_number": result.po_number,
                "total_meters": round(result.total_meters, 0),
                "total_hours": round(result.total_hours, 2),
                **data
            })
        for row in result_dict:
            article = row.get('article')
            if article:
                article_parts = article.split(',')
                row['k1'] = article_parts[0] if len(article_parts) > 0 else None
                row['k2'] = article_parts[1] if len(article_parts) > 1 else None
                row['k3'] = article_parts[2] if len(article_parts) > 2 else None
                row['k4'] = article_parts[3] if len(article_parts) > 3 else None
                row['Finish'] = row['k3']
            else:
                row['k1'], row['k2'], row['k3'], row['k4'], row['Finish'] = [None] * 5
        return result_dict

    except Exception as e:
        print(f"Error: {str(e)}")
        return []


@router.get("/get_monthly_production_of_pad_dying/{date_}")
async def get_monthly_production_of_pad_dying(date_: date, db: Session = Depends(get_db)):
    return await get_monthly_production_of_pad_dying_sheet(db=db, date_=date_)


@router.get("/get_monthly_production_of_dying_report_ftd_and_mtd_data/{date_}")
async def get_monthly_production_of_dying_report_ftd_and_mtd_data(date_: date, db: Session = Depends(get_db)):
    return await get_monthly_production_of_dying_report_ftd_and_mtd(db=db, date_=date_)


@router.get("/get_pad_dry_whole_production_data/{date_}")
async def get_pad_dry_whole_production_data(date_: date, db: Session = Depends(get_db)):
    return await get_pad_dry_whole_production(db=db, date_=date_)


# @router.get("/calculate_stop_production_data/{date_start}/{date_end}")
# async def calculate_stop_production_data(date_start: date, date_end: date, db: Session = Depends(get_db)):
#     return await calculate_stop_production(db=db, date_start=date_start,date_end=date_end)
#
# @router.get("/get_monthly_production_of_dying_report_for_each_date_data/{date_}")
# async def get_monthly_production_of_dying_report_for_each_date_data(date_: date, db: Session = Depends(get_db)):
#     return await get_monthly_production_of_dying_report_for_each_date(db=db, date_=date_)
#
# @router.get("/calculate_utility_production_data/{date_start}/{date_end}")
# async def calculate_utility_production_data(date_start: date, date_end: date, db: Session = Depends(get_db)):
#     return await calculate_utility_production(db=db, date_start=date_start, date_end=date_end)

################### INDU
async def get_monthly_production_of_pad_dying_sheet(db: Session, date_: date):
    try:
        month = date_.month
        current_year = date_.year
        first_day_of_month = datetime.date(datetime(current_year, month, 1))
        last_day_of_month = first_day_of_month.replace(day=calendar.monthrange(current_year, month)[1])
        date_from = first_day_of_month.strftime("%Y-%m-%d")
        date_to = last_day_of_month.strftime("%Y-%m-%d")
        month_name = first_day_of_month.strftime("%B")
        date_range = pd.date_range(date_from, date_to, freq='D')
        all_machines = ['Pad Dry-1', 'Pad Dry-2', 'Pad Dry-3', 'CPB-1', 'CPB-2']
        non_effective_operations = ['PCDISPAD', 'DBDYEING', 'REPADDIN']
        production_dict = {}
        present_date = datetime.today().date()
        for date_data in date_range:
            effective_data_query = db.query(
                models.RunData.machine,
                sqlalchemy.func.sum(models.RunData.meters).label('production')
            ).filter(
                models.RunData.date_ == date_data,
                ~models.RunData.operation_name.in_(non_effective_operations)
            ).group_by(models.RunData.machine).all()

            disperse_non_effective_data_query = db.query(models.RunData.machine,
                                                         sqlalchemy.func.sum(models.RunData.meters).label('production')
                                                         ).filter(
                models.RunData.date_ == date_data,
                models.RunData.operation_name == "PCDISPAD"
            ).group_by(models.RunData.machine).all()

            double_dyeing_non_effective_data_query = db.query(models.RunData.machine,
                                                              sqlalchemy.func.sum(models.RunData.meters).label(
                                                                  'production')
                                                              ).filter(
                models.RunData.date_ == date_data,
                models.RunData.operation_name == "DBDYEING"
            ).group_by(models.RunData.machine).all()

            re_dyeing_non_effective_data_query = db.query(models.RunData.machine,
                                                          sqlalchemy.func.sum(models.RunData.meters).label(
                                                              'production')
                                                          ).filter(
                models.RunData.date_ == date_data,
                models.RunData.operation_name == "REPADDIN"
            ).group_by(models.RunData.machine).all()

            for machine in all_machines:
                if effective_data_query:
                    effective_production_value = next(
                        (production for m, production in effective_data_query if m == machine), 0)
                    disperse_non_effective_production_value = next(
                        (production for m, production in disperse_non_effective_data_query if m == machine), 0)
                    double_dyeing_non_effective_production_value = next(
                        (production for m, production in double_dyeing_non_effective_data_query if m == machine), 0)
                    re_dyeing_non_effective_production_value = next(
                        (production for m, production in re_dyeing_non_effective_data_query if m == machine), 0)


                else:
                    effective_production_value = 0
                    disperse_non_effective_production_value = 0
                    double_dyeing_non_effective_production_value = 0
                    re_dyeing_non_effective_production_value = 0

                # if date_data.date() == present_date:
                #     production_value = 0

                data_production = {
                    "date": date_data.strftime('%Y-%m-%d'),
                    "machine": machine,
                    "effective_production_value": int(effective_production_value),
                    "disperse_non_effective_production_value": int(disperse_non_effective_production_value),
                    "double_dyeing_non_effective_production_value": int(double_dyeing_non_effective_production_value),
                    "re_dyeing_non_effective_production_value": int(re_dyeing_non_effective_production_value)
                }

                if machine not in production_dict:
                    production_dict[machine] = []

                production_dict[machine].append(data_production)

        return production_dict

    except Exception as e:
        print(e)


async def calculate_ftd_and_mtd_production(db: Session, date_start, date_end, machines, operation):
    query = db.query(
        sqlalchemy.func.sum(models.RunData.meters).label('production')
    ).filter(
        models.RunData.date_.between(date_start, date_end),
        models.RunData.machine.in_(machines),
        models.RunData.operation_name.in_(operation)
    )
    total_data = query.all()
    if total_data and total_data[0][0] is not None:
        return total_data[0][0]
    else:
        return 0


async def calculate_total_production(db: Session, date_start, date_end, machines):
    query = db.query(
        sqlalchemy.func.sum(models.RunData.meters).label('production')
    ).filter(
        models.RunData.date_.between(date_start, date_end),
        models.RunData.machine.in_(machines)
    )
    total_data = query.all()
    if total_data and total_data[0][0] is not None:
        return total_data[0][0]
    else:
        return 0


async def calculate_non_effective_production(db: Session, date_start, date_end, machines, operation):
    query = db.query(
        sqlalchemy.func.sum(models.RunData.meters).label('production')
    ).filter(
        models.RunData.date_.between(date_start, date_end),
        models.RunData.machine.in_(machines),
        models.RunData.operation_name.in_(operation)
    )
    total_data = query.all()
    if total_data and total_data[0][0] is not None:
        return total_data[0][0]
    else:
        return 0


async def calculate_stop_production(db: Session, date_start, date_end):
    global machine_name
    categories = ['NO PRG', 'Electrical Breakdown', 'Mechanical Breakdown', 'M/C Maintenance', 'Major Concern',
                  'ANY PROCESS ABNOMALITY DEVIATION']
    pad_dry_machine_list = ['Pad Dry-1', 'Pad Dry-2', 'Pad Dry-3']
    breakdown_data_by_machine = {}

    for machine in pad_dry_machine_list:
        breakdown_hours_data = []
        for category in categories:
            query = db.query(
                models.StopData.machine,
                cast(sqlalchemy.func.sum(models.StopData.duration) / 3600.0, Float).label(f'{category}_hours'),
            ).filter(
                models.StopData.stop_category == category,
                models.StopData.date_.between(date_start, date_end),
                models.StopData.machine == machine
            ).group_by(models.StopData.machine)

            data = query.all()

            if data:
                category_data = data[0][1] if data[0][1] is not None else 0
                breakdown_hours_data.append(int(category_data))
            else:
                breakdown_hours_data.append(0)

        # Once all categories for the machine are processed, store the data in the dictionary
        breakdown_data_by_machine[machine] = {
            "machine": machine,
            "no_prg": breakdown_hours_data[0],
            "electrical_data": breakdown_hours_data[1],
            "mechanical_data": breakdown_hours_data[2],
            "mc_maintenance": breakdown_hours_data[3],
            "major_concern": breakdown_hours_data[4],
            "any_process_abnomality_deviation": breakdown_hours_data[5]
        }

    # Convert dictionary values to list of breakdown data for each machine
    breakdown_hours_list = list(breakdown_data_by_machine.values())

    return breakdown_hours_list


async def get_monthly_production_of_dying_report_for_each_date(db: Session, date_: date):
    try:
        month = date_.month
        current_year = date_.year
        first_day_of_month = datetime.date(datetime(current_year, month, 1))
        last_day_of_month = first_day_of_month.replace(day=calendar.monthrange(current_year, month)[1])
        date_from = first_day_of_month.strftime("%Y-%m-%d")
        date_to = last_day_of_month.strftime("%Y-%m-%d")
        month_name = first_day_of_month.strftime("%B")
        date_range = pd.date_range(date_from, date_to, freq='D')
        pad_dry_machines = ['Pad Dry-1', 'Pad Dry-2', 'Pad Dry-3']
        operations_list = ['PCVATPAD', 'CHEMPAD', 'DBDYEING', 'CPBPAD', 'ECONTROL', 'DISVTPAD',
                           'REPADDIN']
        total_production_dict = {}
        present_date = datetime.today().date()
        production_data_by_date = {}

        for date_data in date_range:
            date_str = date_data.strftime('%Y-%m-%d')
            production_data_by_date[date_str] = {}
            for operation in operations_list:
                operation_production_query = db.query(
                    models.RunData.operation_name,
                    func.sum(models.RunData.meters).label('total_production')
                ).filter(
                    models.RunData.date_ == date_data,
                    models.RunData.operation_name == operation,
                    models.RunData.machine.in_(pad_dry_machines)
                ).group_by(models.RunData.operation_name).first()

                total_production = operation_production_query.total_production if operation_production_query else 0
                production_data_by_date[date_str][operation] = total_production

        return production_data_by_date


    except Exception as e:
        print(e)


async def get_machine_utility(db: Session, date_start, date_end, machine_group, group_name,
                              utility_data_by_machine_list):
    total_utility = 0  # Variable to hold the sum of utility for the group
    for machine in machine_group:
        query = db.query(
            models.RunData.machine,
            cast(func.sum(models.RunData.duration) / (1440 * 60), Float).label(f'{machine}_utility')
        ).filter(
            models.RunData.date_.between(date_start, date_end),
            models.RunData.machine == machine  # Filter for each machine in the group
        ).group_by(models.RunData.machine).first()

        # Extract utility value for each machine, defaulting to 0 if not found
        machine_utility = query[1] if query and query[1] is not None else 0
        total_utility += machine_utility  # Sum the utility for the group

    # Store the total utility for the machine group
    utility_data_by_machine_list[group_name] = {
        "machine_list": group_name,
        "total_utility": round(total_utility, 2)
    }


async def calculate_utility_production(db: Session, date_start, date_end):
    # Define machine lists
    paddry_machine_list = ['Pad Dry-1', 'Pad Dry-2', 'Pad Dry-3']
    cpb_machine_list = ['CPB-1', 'CPB-2']
    jigger_machine_list = ['Jigger-1', 'Jigger-2', 'Jigger-3']
    jet_machine_list = ['Sclavos-1', 'Sclavos-2', 'Sclavos-3', 'Sclavos-4']

    # Additional machine lists
    ops_machine_list = ['OPS']
    kps_machine_list = ['KPS']
    relax_washer_machine_list = ['Relax-Washer']
    kusters_washer_machine_list = ['KUSTERS WASHER']
    thermofix_machine_list = ['Thermofix']
    utility_data_by_machine_list = {}
    # Calculate utility for each machine group
    await get_machine_utility(db, date_start, date_end, paddry_machine_list, "Pad Dry", utility_data_by_machine_list)
    await get_machine_utility(db, date_start, date_end, cpb_machine_list, "CPB", utility_data_by_machine_list)
    await get_machine_utility(db, date_start, date_end, ops_machine_list, "OPS", utility_data_by_machine_list)
    await get_machine_utility(db, date_start, date_end, kps_machine_list, "KPS", utility_data_by_machine_list)
    await get_machine_utility(db, date_start, date_end, relax_washer_machine_list, "Relax-Washer",
                              utility_data_by_machine_list)
    await get_machine_utility(db, date_start, date_end, kusters_washer_machine_list, "Kusters Washer",
                              utility_data_by_machine_list)
    await get_machine_utility(db, date_start, date_end, thermofix_machine_list, "Thermofix",
                              utility_data_by_machine_list)
    await get_machine_utility(db, date_start, date_end, jigger_machine_list, "Jigger", utility_data_by_machine_list)
    await get_machine_utility(db, date_start, date_end, jet_machine_list, "Sclavos", utility_data_by_machine_list)

    # Convert dictionary values to a list for easier consumption
    utility_data_list = list(utility_data_by_machine_list.values())

    return utility_data_list


async def get_monthly_production_of_dying_report_ftd_and_mtd(db: Session, date_: date):
    try:
        month = date_.month
        current_year = date_.year
        first_day_of_month = datetime.date(datetime(current_year, month, 1))
        last_day_of_month = first_day_of_month.replace(day=calendar.monthrange(current_year, month)[1])
        date_from = first_day_of_month.strftime("%Y-%m-%d")
        date_to = last_day_of_month.strftime("%Y-%m-%d")
        month_name = first_day_of_month.strftime("%B")
        date_range = pd.date_range(date_from, date_to, freq='D')
        all_machines = ['Pad Dry-1', 'Pad Dry-2', 'E-Control', 'CPB-1', 'CPB-2', 'Jigger-1', 'Jigger-2', 'Jigger-3',
                        'Sclavos-1', 'Sclavos-2', 'Sclavos-3', 'Sclavos-4',
                        'OPS', 'KPS', 'E-Control', 'Relax-Washer']
        pad_dry_machines = ['Pad Dry-1', 'Pad Dry-2', 'E-Control']
        non_effective_operations = ['Disperse padding', 'Double Dyeing']
        production_dict = {}
        present_date = datetime.today().date()
        first_date_of_month = date_.replace(day=1)

        re_dyeing_today = await calculate_ftd_and_mtd_production(db, date_, date_, pad_dry_machines, ['REPADDIN'])
        disperse_padding_today = await calculate_ftd_and_mtd_production(db, date_, date_, pad_dry_machines,
                                                                        ['PCDISPAD'])
        re_dyeing_mtd = await calculate_ftd_and_mtd_production(db, first_date_of_month, date_, pad_dry_machines,
                                                               ['REPADDIN'])
        disperse_padding_mtd = await calculate_ftd_and_mtd_production(db, first_date_of_month, date_, pad_dry_machines,
                                                                      ['PCDISPAD'])
        total_paddry_prodcution_today = await calculate_total_production(db, date_, date_, pad_dry_machines)
        total_paddry_prodcution_mtd = await calculate_total_production(db, first_date_of_month, date_, pad_dry_machines)
        fresh_production_today = total_paddry_prodcution_today - (re_dyeing_today + disperse_padding_today)
        fresh_production_mtd = total_paddry_prodcution_mtd - (re_dyeing_mtd + disperse_padding_mtd)

        ops_production_today = await calculate_total_production(db, date_, date_, ['OPS'])
        ops_production_mtd = await calculate_total_production(db, first_date_of_month, date_, ['OPS'])
        kps_production_today = await calculate_total_production(db, date_, date_, ['KPS'])
        kps_production_mtd = await calculate_total_production(db, first_date_of_month, date_, ['KPS'])
        relax_washer_production_today = await calculate_total_production(db, date_, date_, ['Relax-Washer'])
        relax_washer_production_mtd = await calculate_total_production(db, first_date_of_month, date_, ['Relax-Washer'])
        thermofix_production_today = await calculate_total_production(db, date_, date_, ['Thermofix'])
        thermofix_production_mtd = await calculate_total_production(db, first_date_of_month, date_, ['Thermofix'])
        jiger_production_today = await calculate_total_production(db, date_, date_,
                                                                  ['Jigger-1', 'Jigger-2', 'Jigger-3'])
        jiger_production_mtd = await calculate_total_production(db, first_date_of_month, date_,
                                                                ['Jigger-1', 'Jigger-2', 'Jigger-3'])
        jet_production_today = await calculate_total_production(db, date_, date_,
                                                                ['Sclavos-1', 'Sclavos-2', 'Sclavos-3',
                                                                 'Sclavos-4'])
        jet_production_mtd = await calculate_total_production(db, first_date_of_month, date_,
                                                              ['Sclavos-1', 'Sclavos-2', 'Sclavos-3',
                                                               'Sclavos-4'])
        double_dyeing_production_today = await calculate_non_effective_production(db, date_, date_, all_machines,
                                                                                  ['DBDYEING'])
        double_dyeing_production_mtd = await calculate_non_effective_production(db, first_date_of_month, date_,
                                                                                all_machines, ['DBDYEING'])
        stopage_data = await calculate_stop_production(db=db, date_start=date_, date_end=date_)
        monthly_production = await get_monthly_production_of_dying_report_for_each_date(db, date_)
        utility_data = await calculate_utility_production(db, date_, date_)

        data_production = {
            "fresh_production_today": int(fresh_production_today),
            "fresh_production_mtd": int(fresh_production_mtd),
            "re_dyeing_today": int(re_dyeing_today),
            "re_dyeing_mtd": int(re_dyeing_mtd),
            "disperse_padding_today": int(disperse_padding_today),
            "disperse_padding_mtd": int(disperse_padding_mtd),
            "ops_production_today": int(ops_production_today),
            "ops_production_mtd": int(ops_production_mtd),
            "kps_production_today": int(kps_production_today),
            "kps_production_mtd": int(kps_production_mtd),
            "relax_washer_production_today": int(relax_washer_production_today),
            "relax_washer_production_mtd": int(relax_washer_production_mtd),
            "thermofix_production_today": int(thermofix_production_today),
            "thermofix_production_mtd": int(thermofix_production_mtd),
            "jiger_production_today": int(jiger_production_today),
            "jiger_production_mtd": int(jiger_production_mtd),
            "jet_production_today": int(jet_production_today),
            "jet_production_mtd": int(jet_production_mtd),
            "double_dyeing_production_today": int(double_dyeing_production_today),
            "double_dyeing_production_mtd": int(double_dyeing_production_mtd),
            "stopage_data": stopage_data,
            "monthly_production": monthly_production,
            "utility_data": utility_data
        }

        return data_production

    except Exception as e:
        print(e)


async def get_pad_dry_whole_production(db: Session, date_: date):
    try:
        machines_list = ['Pad Dry-1', 'Pad Dry-2', 'Pad Dry-3', 'CPB-1', 'CPB-2']
        pad_dry_machine_mapping = {
            'Pad Dry-1': 'FPD00101',
            'Pad Dry-2': 'FPD00102',
            'Pad Dry-3': 'FPD00103',
            'CPB-1': 'FCD00101',
            'CPB-2': 'FCD00102'

        }

        work_center_name_mapping = {
            'Pad Dry-1': 'Pad Dry',
            'Pad Dry-2': 'Pad Dry',
            'Pad Dry-3': 'Pad Dry',
            'CPB-1': 'Cold Pad Batch',
            'CPB-2': 'Cold Pad Batch'

        }

        unique_po_number = db.query(models.RunData.po_number).filter(
            models.RunData.date_ == date_,
            models.RunData.duration > 0.0028,
            models.RunData.machine.in_(machines_list)
        ).distinct().all()

        pad_dry_production_data = []
        if unique_po_number:
            unique_po_number_list = [item[0] for item in unique_po_number]

            serial_number = 1
            for po_num in unique_po_number_list:

                run_data = db.query(
                    models.RunData.po_number, sqlalchemy.func.sum(models.RunData.meters).label('production'),
                    models.RunData.operation_name, models.RunData.shift, models.RunData.machine,
                    models.RunData.stop_time
                ).filter(
                    models.RunData.machine.in_(machines_list),
                    models.RunData.date_ == date_,
                    models.RunData.po_number == po_num
                ).group_by(
                    models.RunData.po_number,
                    models.RunData.operation_name,
                    models.RunData.shift,
                    models.RunData.machine,
                    models.RunData.stop_time
                ).first()

                po_data = db.query(models.PoData.po_number,
                                   models.PoData.article,
                                   models.PoData.greige_glm).filter(models.PoData.po_number == po_num).first()

                glm = 0
                if run_data:
                    po_number = run_data[0]
                    meters_production = int(run_data[1]) if run_data[1] else 0
                    operation_name = run_data[2] if run_data[2] else " "
                    shift_data = run_data[3] if run_data[3] else " "
                    machine = run_data[4] if run_data[4] else " "
                    machine_code = pad_dry_machine_mapping.get(machine, "Unknown Machine Code")
                    work_center_name = work_center_name_mapping.get(machine, "Unknown Machine name")

                    stop_time = run_data[5] if run_data[5] else " "
                    duration_subquery = db.query(models.RunData.machine, models.RunData.po_number,
                                                 (func.sum(models.RunData.duration) / 60).label('total_duration')
                                                 ).join(models.PoData,
                                                        (models.RunData.po_number == models.PoData.po_number) & (
                                                                models.RunData.machine == models.PoData.machine)
                                                        ).filter(models.RunData.date_ == date_,
                                                                 models.RunData.machine.in_(machines_list),
                                                                 models.RunData.po_number == po_number,
                                                                 models.RunData.operation_name == operation_name).group_by(
                        models.RunData.machine,
                        models.RunData.po_number).subquery()
                    total_duration = db.query(duration_subquery).all()

                    speed_subquery = db.query(models.RunData.machine, models.RunData.po_number,
                                              (func.sum(models.RunData.meters) / (
                                                      func.sum(models.RunData.duration) / 60)).label('speed')
                                              ).join(models.PoData,
                                                     (models.RunData.po_number == models.PoData.po_number) & (
                                                             models.RunData.machine == models.PoData.machine)
                                                     ).filter(
                        models.RunData.date_ == date_, models.RunData.machine.in_(machines_list),
                        models.RunData.po_number == po_number, models.RunData.operation_name == operation_name
                    ).group_by(models.RunData.machine, models.RunData.po_number).subquery()
                    speed = db.query(speed_subquery).all()

                    run_duration = total_duration[0][2] if total_duration else 0

                    if po_data and po_data[1] and po_data[2]:
                        article_string = po_data[1]
                        article_parts = article_string.split(',')
                        keys = ['k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7', 'k8', 'k9']
                        article_dict = {key: value for key, value in zip(keys, article_parts)}
                        glm = po_data[2] or 0

                        pad_dry_production_data.append({
                            'machine': machine,
                            'date_': date_,
                            'stop_time': stop_time.strftime("%H:%M:%S"),
                            's_no': serial_number,
                            'article': article_dict.get('k1', ''),
                            'variants_data': article_dict.get('k2', ''),
                            'finish': article_dict.get('k3', ''),
                            'shade_no': article_dict.get('k4', ''),
                            'po_number': po_number,
                            'shift_data': shift_data,
                            'meter_production': meters_production,
                            'operation_name': operation_name,
                            'run_duration': run_duration,
                            'speed': speed[0][2] if speed else 0,
                            'glm': glm,
                            'machine_code': machine_code,
                            'work_center_code': 'FCD001',
                            'work_center_name': work_center_name
                        })


                    else:
                        print(f"No data found in PoData for po_number {po_num}")
                        # Handle case where PoData does not have matching po_number
                        pad_dry_production_data.append({
                            'machine': machine,
                            "date_": date_,
                            'stop_time': stop_time.strftime("%H:%M:%S"),
                            'shift_data': shift_data,
                            's_no': serial_number,
                            'article': " ",
                            'variants_data': " ",
                            'po_number': po_number,
                            'finish': " ",
                            'shade_no': " ",
                            'meter_production': meters_production,
                            'operation_name': operation_name,
                            'run_duration': total_duration[0][2] if total_duration else 0,
                            'speed': speed[0][2] if speed else 0,
                            'glm': glm,
                            'machine_code': machine_code,
                            'work_center_code': 'FCD001',
                            'work_center_name': work_center_name
                        })

                else:
                    print(f"No data found in RunData for po_number {po_num}")
                    # Handle case where RunData does not have matching po_number
                    pad_dry_production_data.append({
                        'machine': machine,
                        "date_": date_,
                        'stop_time': stop_time.strftime("%H:%M:%S"),
                        'shift_data': shift_data,
                        's_no': serial_number,
                        'article': " ",
                        'variants_data': " ",
                        'po_number': po_num,
                        'finish': " ",
                        'shade_no': " ",
                        'meter_production': meters_production,
                        'operation_name': operation_name,
                        "run_duration": total_duration[0][2] if total_duration else 0,
                        "speed": speed[0][2] if speed else 0,
                        'glm': glm,
                        'machine_code': machine_code,
                        'work_center_code': 'FCD001',
                        'work_center_name': work_center_name
                    })

                serial_number += 1  # Increment serial number for next iteration

            return pad_dry_production_data

        else:
            pad_dry_production_data.append({
                "machine": " ",
                "date_": " ",
                'stop_time': " ",
                "s_no": " ",
                'article': " ",
                'po_number': " ",
                'finish': " ",
                'variants_data': " ",
                'shade_no': " ",
                'meter_production': " ",
                'operation_name': " ",
                "run_duration": " ",
                "speed": " ",
                "glm": " ",
                'machine_code': " ",
                'work_center_code': " ",
                'work_center_name': " ",
                'shift_data': " "

            })
            return pad_dry_production_data

    except Exception as e:
        print(e)
