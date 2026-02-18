from collections import defaultdict
from sqlalchemy import extract, func, and_, case
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Session
import sqlalchemy
from . import models, schemas
import pandas as pd
from typing import Dict
from datetime import date, timedelta, datetime
import calendar
from datetime import timedelta
from fastapi import HTTPException
from sqlalchemy.sql import func
import json
from sqlalchemy import cast, Float, text
from sqlalchemy import func, extract
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import JSONB, TEXT
import json
from datetime import date
import json
from sqlalchemy import func, extract
from sqlalchemy.dialects.postgresql import JSONB


async def get_run_category(db: Session, name: str):
    return db.query(models.RunCategory).filter(models.RunCategory.name == name).first()


async def get_stop_per_day(db: Session, start_date: date, end_date: date, machine: str):
    stm = sqlalchemy.select(
        sqlalchemy.func.max(models.StopData.machine).label('machine'),
        models.StopData.stop_category,
        sqlalchemy.func.sum(models.StopData.duration).label('duration_sum'),
        sqlalchemy.func.count(models.StopData.stop_category).label('stoppage_count')
    ).filter(models.StopData.date_.between(start_date, end_date),
             models.StopData.duration >= 60,
             models.StopData.machine == machine
             ).group_by(models.StopData.stop_category
                        ).order_by(sqlalchemy.func.sum(models.StopData.duration).desc()
                                   ).subquery()
    return db.query(stm).all()


async def get_run_per_day(db: Session, start_date: date, end_date: date, machine: str):
    stm1 = sqlalchemy.select(
        sqlalchemy.func.max(models.RunData.date_).label('date_'),
        sqlalchemy.func.min(models.RunData.start_time).label('start_time'),
        sqlalchemy.func.max(models.RunData.stop_time).label('end_time'),
        sqlalchemy.func.max(models.RunData.po_number).label('po_number'),
        sqlalchemy.func.max(models.RunData.shift).label('shift'),
        sqlalchemy.func.sum(models.RunData.meters).label('production'),
        (sqlalchemy.func.sum(models.RunData.meters) / sqlalchemy.func.sum(models.RunData.duration) * 60).label('speed'),
    ).filter(models.RunData.date_.between(start_date, end_date),
             models.RunData.meters > 1,
             models.RunData.run_category != "Lead cloth",
             models.RunData.machine == machine
             ).group_by(models.RunData.po_number
                        ).order_by(sqlalchemy.func.max(models.RunData.date_).asc(),
                                   sqlalchemy.func.max(models.RunData.shift).asc(),
                                   sqlalchemy.func.max(models.RunData.time_).asc(),
                                   ).subquery()
    run_po_data = db.query(stm1).all()
    run_po_data_df = pd.DataFrame(run_po_data, columns=['date_', 'start_time',
                                                        'end_time', 'po_number', 'shift',
                                                        'production', 'speed'])
    # print(run_po_data_df.head())
    sorted_df = run_po_data_df.sort_values(['date_', 'shift', 'start_time'])
    try:
        sorted_df['production'] = sorted_df['production'].astype(int)
    except:
        pass
    try:
        sorted_df['speed'] = sorted_df['speed'].astype(int)
    except:
        pass
    return sorted_df.to_dict("records")


async def get_time_sequence(db: Session, start_date: date, end_date: date, machine: str):
    run_stm = sqlalchemy.select(
        models.RunData.date_,
        models.RunData.shift,
        models.RunData.start_time,
        models.RunData.stop_time,
        models.RunData.duration,
        models.RunData.run_category.label("category"),
        models.RunData.po_number,
        sqlalchemy.sql.expression.literal_column("1").label("status")
    ).filter(models.RunData.date_.between(start_date, end_date),
             models.RunData.stop_time != None,
             models.RunData.duration != None,
             models.RunData.duration >= 60,
             models.RunData.machine == machine
             ).order_by(models.RunData.run_data_id.asc()
                        ).subquery()
    stop_stm = sqlalchemy.select(
        models.StopData.date_,
        models.StopData.shift,
        models.StopData.start_time,
        models.StopData.stop_time,
        models.StopData.duration,
        models.StopData.stop_category.label("category"),
        models.StopData.po_number,
        sqlalchemy.sql.expression.literal_column("0").label("status")
    ).filter(models.StopData.date_.between(start_date, end_date),
             models.StopData.stop_time != None,
             models.StopData.duration != None,
             models.StopData.duration >= 60,
             models.StopData.machine == machine
             ).order_by(models.StopData.stop_data_id.asc()).subquery()
    hex_map = {'Bulk Dyed': '#33CC33', 'Bulk Yarn Dyed': '#33CC33', 'Bulk Full Bleach': '#33CC33',
               'Bulk RFD': '#33CC33', 'Reprocess': '#33CC33', 'Yardage': '#33CC33', 'BL': '#33CC33', 'RFS': '#33CC33',
               'Process demand': '#33CC33', 'Trial Maintenance': '#D0CECE', 'Lead cloth': '#FF3399',
               'Machine cleaning': '#FF3399', 'Fabric changeover': '#FF3399', 'Startup for Yardage': '#FF3399',
               'Radiator cleaning': '#FF3399', 'Preventive Maintenance': '#0080FF',
               'Morning machine cleaning': '#0080FF', 'No Program': '#0040FF', 'No StopType': '#0040FF',
               'Man power shortage': '#FFFF00', 'Trolley/Batch Not Available': '#FFC000',
               'Quality fail due to process': '#FFC000', 'Quality fail due to mc': '#D00D00',
               'Electrical Breakdown': '#D00D00', 'Mechanical Breakdown': '#D00D00', 'Utility Breakdown': '#D00D00',
               'Corrective Maintenance': '#D00D00', 'Low Air Pressure': '#D00D00', 'No Water': '#D00D00',
               'Lead cloth burst': '#D00D00', 'Chemical issue': '#D00D00', 'Pin Bar change': '#D00D00',
               'Nip Test': '#D00D00', 'Teflon finish cleaning': '#D00D00', 'Tank Not ok': '#D00D00',
               'NO REASON SELECTED': '#D00D00',
               }
    time_data = db.query(run_stm).all()
    stop_data = db.query(stop_stm).all()
    time_data = time_data + stop_data
    # pprint(time_data)
    time_data_df = pd.DataFrame(time_data, columns=['date_', 'shift', 'start_time',
                                                    'stop_time', 'duration', 'category',
                                                    'po_number', 'status'])
    # print(time_data_df.head())
    sorted_df = (time_data_df.sort_values('start_time'))
    sorted_df['color'] = sorted_df['category'].map(hex_map).astype(str)
    # print(time_data_df.head())
    # pprint(time_data_df.to_dict("records"))
    return sorted_df.to_dict("records")


async def get_stop_category(db: Session, name: str):
    return db.query(models.StopCategory).filter(models.StopCategory.name == name).first()


async def get_operator(db: Session, name: str):
    return db.query(models.OperatorList).filter(models.OperatorList.name == name).first()


async def get_run_by_id(db: Session, run_data_id: int, machine: str):
    return db.query(models.RunData).filter(models.RunData.run_data_id == run_data_id,
                                           models.RunData.machine == machine).first()


async def get_stop_by_id(db: Session, stop_data_id: int, machine: str):
    return db.query(models.StopData).filter(models.StopData.stop_data_id == stop_data_id,
                                            models.StopData.machine == machine).first()


async def create_run_category(db: Session, run_category: schemas.RunCategoryBase):
    db_run_cat = models.RunCategory(**run_category.dict())
    print(db_run_cat)
    db.add(db_run_cat)
    db.commit()
    db.refresh(db_run_cat)
    return db_run_cat


async def create_stop_category(db: Session, stop_category: schemas.StopCategoryBase):
    db_stop_cat = models.StopCategory(**stop_category.dict())
    db.add(db_stop_cat)
    db.commit()
    db.refresh(db_stop_cat)
    return db_stop_cat


async def create_operator_list(db: Session, operator_list: schemas.OperatorListBase):
    db_operator_lst = models.OperatorList(operator_num=operator_list.operator_num, name=operator_list.name)
    db.add(db_operator_lst)
    db.commit()
    db.refresh(db_operator_lst)
    return db_operator_lst


async def create_run_data(db: Session, run_data: schemas.RunDataCreate):
    db_run_data = models.RunData(**run_data.dict())
    db.add(db_run_data)
    db.commit()
    db.refresh(db_run_data)
    return db_run_data


async def update_run_data(db: Session, run_data: schemas.RunDataUpdate):
    run_data_db = await get_run_by_id(db, run_data.run_data_id, run_data.machine)
    r_id = run_data_db.id
    # print(p_id)
    db_run = db.get(models.RunData, r_id)
    for key, value in run_data.dict(exclude_unset=True).items():
        setattr(db_run, key, value)
    db.add(db_run)
    db.commit()
    db.refresh(db_run)
    return db_run


async def create_stop_data(db: Session, stop_data: schemas.StopDataCreate):
    db_stop_data = models.StopData(**stop_data.dict())
    db.add(db_stop_data)
    db.commit()
    db.refresh(db_stop_data)
    return db_stop_data


async def update_stop_data(db: Session, stop_data: schemas.StopDataUpdate):
    stop_data_db = await get_stop_by_id(db, stop_data.stop_data_id, stop_data.machine)
    s_id = stop_data_db.id
    # print(s_id)
    db_stop = db.get(models.StopData, s_id)
    # print(db_stop)
    for key, value in stop_data.dict(exclude_unset=True).items():
        setattr(db_stop, key, value)
    db.add(db_stop)
    db.commit()
    db.refresh(db_stop)
    return db_stop


# ...................................Finishing Section Prod...............................

async def get_report_combined_production(db: Session, date_: date) -> Dict[str, Dict[str, int]]:
    try:

        query_all_machines = db.query(
            models.RunData.shift,
            models.RunData.machine,
            sqlalchemy.func.sum(models.RunData.meters).label('production'),
        ).filter(
            models.RunData.date_ == date_
        ).group_by(models.RunData.shift, models.RunData.machine)

        data_all_machines = query_all_machines.all()

        result_data = {}

        for shift, machine, production in data_all_machines:

            if machine not in result_data:
                result_data[machine] = {}

            result_data[machine][shift] = production

        query_stenter5 = db.query(
            models.RunData.shift,
            models.RunData.machine,
            models.RunData.operation_name,
            sqlalchemy.func.sum(models.RunData.meters).label('production'),
        ).filter(
            models.RunData.date_ == date_,
            models.RunData.machine == 'Stenter-5'
        ).group_by(models.RunData.shift, models.RunData.machine, models.RunData.operation_name)

        data_stenter5 = query_stenter5.all()

        for shift, machine, operation_name, production in data_stenter5:

            heading = f"{machine}({operation_name})"

            if heading not in result_data:
                result_data[heading] = {}

            result_data[heading][shift] = production

        return result_data

    except Exception as e:
        print(e)


async def get_report_hot_and_heat_set_data(db: Session, date_: date):
    try:

        result_dict = {}

        hot_flue_query = db.query(
            models.RunData.machine,
            sqlalchemy.func.sum(models.RunData.meters).label('production_of_Hot_Flue'),
        ).filter(
            models.RunData.date_ == date_,
            models.RunData.run_category == 'Hot Flue'
        ).group_by(models.RunData.machine)

        for machine, production_of_hot_flue in hot_flue_query.all():
            result_dict[machine] = {
                'machine': machine,
                'production_of_Hot_Flue': production_of_hot_flue,
            }

        hot_flue_heat_stenter5 = db.query(
            models.RunData.machine,
            models.RunData.operation_name,
            sqlalchemy.func.sum(models.RunData.meters).label('production_of_Hot_Flue_Heat_Set_Stenter5'),
        ).filter(
            models.RunData.date_ == date_,
            models.RunData.run_category == 'Hot Flue',
            models.RunData.operation_name == 'HEATSETT',
            models.RunData.machine == 'Stenter-5'
        ).group_by(models.RunData.machine, models.RunData.operation_name)

        for machine, operation_name, production_of_hot_flue_heat_set_stenter5 in hot_flue_heat_stenter5.all():
            result_dict[f"{machine}({operation_name})"] = {
                'machine': f"{machine}({operation_name})",
                'production_of_Hot_Flue': production_of_hot_flue_heat_set_stenter5
            }

        hot_flue_finish_stenter5 = db.query(
            models.RunData.machine,
            models.RunData.operation_name,
            sqlalchemy.func.sum(models.RunData.meters).label('production_of_Hot_Flue_Finish_Stenter5'),
        ).filter(
            models.RunData.date_ == date_,
            models.RunData.run_category == 'Hot Flue',
            models.RunData.operation_name == 'FINISHNG',
            models.RunData.machine == 'Stenter-5'
        ).group_by(models.RunData.machine, models.RunData.operation_name)

        for machine, operation_name, production_of_hot_flue_finish_stenter5 in hot_flue_finish_stenter5.all():
            result_dict[f"{machine}({operation_name})"] = {
                'machine': f"{machine}({operation_name})",
                'production_of_Hot_Flue': production_of_hot_flue_finish_stenter5
            }

        heat_set_query = db.query(
            models.RunData.machine,
            sqlalchemy.func.sum(models.RunData.meters).label('production of HEATSET'),
        ).filter(
            models.RunData.date_ == date_,
            models.RunData.operation_name == 'HEATSETT'
        ).group_by(models.RunData.machine)
        for machine, production_of_heat_set in heat_set_query.all():
            result_dict[machine] = {
                'machine': machine,
                'production_of_Heat_Set': production_of_heat_set
            }

        heat_set_heat_stenter5 = db.query(
            models.RunData.machine,
            models.RunData.operation_name,
            sqlalchemy.func.sum(models.RunData.meters).label('production_of_Heat_Set_Stenter5'),
        ).filter(
            models.RunData.date_ == date_,

            models.RunData.operation_name == 'HEATSETT',
            models.RunData.machine == 'Stenter-5'
        ).group_by(models.RunData.machine, models.RunData.operation_name)

        for machine, operation_name, production_of_Heat_Set_Stenter5 in heat_set_heat_stenter5.all():
            result_dict[f"{machine}({operation_name})"] = {
                'machine': f"{machine}({operation_name})",
                'production_of_Heat_Set': production_of_Heat_Set_Stenter5
            }

        heat_set_finish_stenter5 = db.query(
            models.RunData.machine,
            models.RunData.operation_name,
            sqlalchemy.func.sum(models.RunData.meters).label('production_of_Heat_Set_Finish_Stenter5'),
        ).filter(
            models.RunData.date_ == date_,
            models.RunData.operation_name == 'HEATSETT',
            models.RunData.operation_name == 'FINISHNG',
            models.RunData.machine == 'Stenter-5'
        ).group_by(models.RunData.machine, models.RunData.operation_name)

        for machine, operation_name, production_of_Heat_Set_Finish_Stenter5 in heat_set_finish_stenter5.all():
            result_dict[f"{machine}({'Finish'})"] = {
                'machine': f"{machine}({'Finish'})",
                'production_of_Heat_Set': production_of_Heat_Set_Finish_Stenter5
            }

        return result_dict

    except Exception as e:
        print(f"Error: {e}")


async def get_report_data_till_date(db: Session, date_: date):
    try:
        month = extract('month', date_)
        year = extract('year', date_)

        query_all_machines = db.query(
            models.RunData.machine,
            sqlalchemy.func.sum(models.RunData.meters).label('production'),
        ).filter(
            extract('month', models.RunData.date_) == month,
            extract('year', models.RunData.date_) == year,
            models.RunData.date_ <= date_
        ).group_by(models.RunData.machine)

        data_all_machines = query_all_machines.all()

        result_data = {}

        for machine, production in data_all_machines:
            result_data[machine] = {"production": production}

        query_stenter5 = db.query(
            models.RunData.machine,
            models.RunData.operation_name,
            sqlalchemy.func.sum(models.RunData.meters).label('production'),
        ).filter(
            extract('month', models.RunData.date_) == month,
            extract('year', models.RunData.date_) == year,
            models.RunData.date_ <= date_,
            models.RunData.machine == 'Stenter-5'
        ).group_by(models.RunData.machine, models.RunData.operation_name)

        data_stenter5 = query_stenter5.all()

        for machine, operation_name, production in data_stenter5:
            heading = f"{machine}({operation_name})"
            result_data[heading] = {"production": production}

        return result_data

    except Exception as e:
        print(e)


async def get_report_heat_set_data_till_date(db: Session, date_: date):
    try:

        month = extract('month', date_)
        year = extract('year', date_)

        query = db.query(
            sqlalchemy.func.sum(models.RunData.meters).label('production'),
        ).filter(
            extract('month', models.RunData.date_) == month,
            extract('year', models.RunData.date_) == year,
            models.RunData.date_ <= date_,
            models.RunData.run_category == 'HEATSETT'
        )

        total_production = query.scalar()

        result_data = {'MTD_Heat_Set': total_production}

        return result_data

    except Exception as e:
        print(e)


async def get_report_combined_duration(db: Session, date_: date) -> Dict[str, Dict[str, float]]:
    try:
        query_all_machines = db.query(
            models.RunData.shift,
            models.RunData.machine,
            cast(sqlalchemy.func.sum(models.RunData.duration) / 3600.0, Float).label('production_hours')
        ).filter(
            models.RunData.date_ == date_
        ).group_by(models.RunData.shift, models.RunData.machine)

        data_all_machines = query_all_machines.all()

        result_data = {}

        for shift, machine, production in data_all_machines:
            if machine not in result_data:
                result_data[machine] = {}
            result_data[machine][shift] = round(production, 2)

        query_stenter5 = db.query(
            models.RunData.shift,
            models.RunData.machine,
            models.RunData.operation_name,
            cast(sqlalchemy.func.sum(models.RunData.duration) / 3600.0, Float).label('production_hours')
        ).filter(
            models.RunData.date_ == date_,
            models.RunData.machine == 'Stenter-5'
        ).group_by(models.RunData.shift, models.RunData.machine, models.RunData.operation_name)

        data_stenter5 = query_stenter5.all()

        for shift, machine, operation_name, production in data_stenter5:
            heading = f"{machine}({operation_name})"
            if heading not in result_data:
                result_data[heading] = {}
            result_data[heading][shift] = round(production, 2)

        return result_data

    except Exception as e:
        print(e)

    # async def get_report_combined_duration(db: Session, date_: date) -> Dict[str, Dict[str, int]]:


#    try:
#
#        query_all_machines = db.query(
#            models.RunData.shift,
#            models.RunData.machine,
#            sqlalchemy.func.div(sqlalchemy.func.sum(models.RunData.duration), 3600.0).label('production_hours'),
#        ).filter(
#            models.RunData.date_ == date_
#        ).group_by(models.RunData.shift, models.RunData.machine)
#
#        data_all_machines = query_all_machines.all()
#
#        result_data = {}
#
#        for shift, machine, production in data_all_machines:
#
#            if machine not in result_data:
#                result_data[machine] = {}
#
#            result_data[machine][shift] = production
#
#        query_stenter5 = db.query(
#            models.RunData.shift,
#            models.RunData.machine,
#            models.RunData.operation_name,
#            sqlalchemy.func.div(sqlalchemy.func.sum(models.RunData.duration), 3600.0).label('production_hours'),
#        ).filter(
#            models.RunData.date_ == date_,
#            models.RunData.machine == 'Stenter-5'
#        ).group_by(models.RunData.shift, models.RunData.machine, models.RunData.operation_name)
#
#        data_stenter5 = query_stenter5.all()
#
#        for shift, machine, operation_name, production in data_stenter5:
#
#            heading = f"{machine}({operation_name})"
#
#            if heading not in result_data:
#                result_data[heading] = {}
#
#            result_data[heading][shift] = production
#
#        return result_data
#
#    except Exception as e:
#        print(e)


async def get_report_stop_duration_data(db: Session, date_: date):
    try:
        month = extract('month', date_)
        year = extract('year', date_)

        current_month = date.today().month
        current_date = datetime.now().date()

        mtd_no_prog_query = db.query(
            models.StopData.machine,
            # sqlalchemy.func.div(sqlalchemy.func.sum(models.StopData.duration), 3600).label('mtd_no_prog_hours'),
            cast(sqlalchemy.func.sum(models.StopData.duration) / 3600.0, Float).label('mtd_no_prog_hours'),
        ).filter(
            models.StopData.stop_category == 'No Program',
            extract('month', models.StopData.date_) == current_month,
            # extract('year', models.RunData.date_) == year,
            models.StopData.date_ <= date_

        ).group_by(models.StopData.machine)

        mechanical_query = db.query(
            models.StopData.machine,
            cast(sqlalchemy.func.sum(models.StopData.duration) / 3600.0, Float).label('mechanical_hours'),
        ).filter(
            models.StopData.stop_category == 'Mechanical Breakdown',
            models.StopData.date_ == date_
        ).group_by(models.StopData.machine)

        Fabric_changeover_query = db.query(
            models.StopData.machine,
            cast(sqlalchemy.func.sum(models.StopData.duration) / 3600.0, Float).label('Fabric_changeover_hours'),
        ).filter(
            models.StopData.stop_category == 'Fabric changeover',
            models.StopData.date_ == date_
        ).group_by(models.StopData.machine)

        electrical_query = db.query(
            models.StopData.machine,
            cast(sqlalchemy.func.sum(models.StopData.duration) / 3600.0, Float).label('electrical_hours'),
        ).filter(
            models.StopData.stop_category == 'Electrical Breakdown',
            models.StopData.date_ == date_
        ).group_by(models.StopData.machine)

        no_program_query = db.query(
            models.StopData.machine,
            cast(sqlalchemy.func.sum(models.StopData.duration) / 3600.0, Float).label('no_program_hours'),
        ).filter(
            models.StopData.stop_category == 'No Program',
            models.StopData.date_ == date_
        ).group_by(models.StopData.machine)

        nip_test_query = db.query(
            models.StopData.machine,
            cast(sqlalchemy.func.sum(models.StopData.duration) / 3600.0, Float).label('nip_test_hours'),
        ).filter(
            models.StopData.stop_category == 'Nip Test',
            models.StopData.date_ == date_
        ).group_by(models.StopData.machine)

        lead_cloth_burst_query = db.query(
            models.StopData.machine,
            cast(sqlalchemy.func.sum(models.StopData.duration) / 3600.0, Float).label('lead_cloth_burst_hours'),
        ).filter(
            models.StopData.stop_category == 'Lead cloth stop',
            models.StopData.date_ == date_
        ).group_by(models.StopData.machine)

        startup_for_yardage_query = db.query(
            models.StopData.machine,
            cast(sqlalchemy.func.sum(models.StopData.duration) / 3600.0, Float).label('startup_for_yardage_hours'),
        ).filter(
            models.StopData.stop_category == 'Startup for Yardage',
            models.StopData.date_ == date_
        ).group_by(models.StopData.machine)

        pm_query = db.query(
            models.StopData.machine,
            cast(sqlalchemy.func.sum(models.StopData.duration) / 3600.0, Float).label('pm_hours'),
        ).filter(
            models.StopData.stop_category == 'Preventive Maintenance',
            models.StopData.date_ == date_
        ).group_by(models.StopData.machine)

        pin_query = db.query(
            models.StopData.machine,
            cast(sqlalchemy.func.sum(models.StopData.duration) / 3600.0, Float).label('pin_hours'),
        ).filter(
            models.StopData.stop_category == 'Rubber Grinding/change',
            models.StopData.date_ == date_
        ).group_by(models.StopData.machine)

        air_query = db.query(
            models.StopData.machine,
            cast(sqlalchemy.func.sum(models.StopData.duration) / 3600.0, Float).label('air_hours'),
        ).filter(
            models.StopData.stop_category == 'Power Cut',
            models.StopData.date_ == date_
        ).group_by(models.StopData.machine)

        clg_query = db.query(
            models.StopData.machine,
            cast(sqlalchemy.func.sum(models.StopData.duration) / 3600.0, Float).label('clg_hours'),
        ).filter(
            models.StopData.stop_category == 'Machine cleaning',
            models.StopData.date_ == date_
        ).group_by(models.StopData.machine)

        misc_query = db.query(
            models.StopData.machine,
            cast(sqlalchemy.func.sum(models.StopData.duration) / 3600.0, Float).label('misc_hours'),
        ).filter(
            models.StopData.stop_category == 'Other Faults',
            models.StopData.date_ == date_
        ).group_by(models.StopData.machine)

        oil_query = db.query(
            models.StopData.machine,
            cast(sqlalchemy.func.sum(models.StopData.duration) / 3600.0, Float).label('oil_hours'),
        ).filter(
            models.StopData.stop_category == 'Oil Dropping',
            models.StopData.date_ == date_
        ).group_by(models.StopData.machine)

        mechanical_data = {entry[0]: {"machine": entry[0], "mechanical_hours": entry[1]} for entry in
                           mechanical_query.all()}
        electrical_data = {entry[0]: {"machine": entry[0], "electrical_hours": entry[1]} for entry in
                           electrical_query.all()}
        fabric_changeover_data = {entry[0]: {"machine": entry[0], "fabric_changeover_hours": entry[1]} for entry in
                                  Fabric_changeover_query.all()}
        no_program_data = {entry[0]: {"machine": entry[0], "no_program_hours": entry[1]} for entry in
                           no_program_query.all()}
        nip_test_data = {entry[0]: {"machine": entry[0], "nip_test_hours": entry[1]} for entry in nip_test_query.all()}
        lead_cloth_burst_data = {entry[0]: {"machine": entry[0], "lead_cloth_burst_hours": entry[1]} for entry in
                                 lead_cloth_burst_query.all()}
        startup_for_yardage_data = {entry[0]: {"machine": entry[0], "startup_for_yardage_hours": entry[1]} for entry in
                                    startup_for_yardage_query.all()}
        pm_data = {entry[0]: {"machine": entry[0], "pm_hours": entry[1]} for entry in
                   pm_query.all()}
        pin_data = {entry[0]: {"machine": entry[0], "pin_hours": entry[1]} for entry in
                    pin_query.all()}
        air_data = {entry[0]: {"machine": entry[0], "air_hours": entry[1]} for entry in
                    air_query.all()}

        clg_data = {entry[0]: {"machine": entry[0], "clg_hours": entry[1]} for entry in
                    clg_query.all()}

        misc_data = {entry[0]: {"machine": entry[0], "misc_hours": entry[1]} for entry in
                     misc_query.all()}

        oil_data = {entry[0]: {"machine": entry[0], "oil_hours": entry[1]} for entry in
                    oil_query.all()}

        mtd_no_prog_data = {entry[0]: {"machine": entry[0], "mtd_no_prog_hours": entry[1]} for entry in
                            mtd_no_prog_query.all()}

        # Merge the dictionaries for each machine
        result_data = {machine: {**mechanical_data.get(machine, {}), **electrical_data.get(machine, {}),
                                 **fabric_changeover_data.get(machine, {}), **no_program_data.get(machine, {}),
                                 **nip_test_data.get(machine, {}), **lead_cloth_burst_data.get(machine, {}),
                                 **startup_for_yardage_data.get(machine, {}), **pm_data.get(machine, {}),
                                 **pin_data.get(machine, {}), **air_data.get(machine, {}),
                                 **clg_data.get(machine, {}), **misc_data.get(machine, {}), **oil_data.get(machine, {}),
                                 **mtd_no_prog_data.get(machine, {})
                                 }
                       for machine in set(mechanical_data) | set(electrical_data) | set(fabric_changeover_data)
                       | set(no_program_data) | set(nip_test_data) | set(lead_cloth_burst_data) | set(
                startup_for_yardage_data) | set(pm_data) | set(pin_data) | set(air_data) | set(clg_data) | set(
                misc_data) | set(oil_data) | set(mtd_no_prog_data)}

        return result_data

    except Exception as e:
        print(e)


# ...........................Monthly MC wise speed...........................................


async def get_production_data_for_year(db: Session, date_: date) -> Dict[str, Dict[str, Dict[str, int]]]:
    try:
        year_to_fetch = date_.year
        result_data_for_year = {}

        current_date = date(year_to_fetch, 1, 1)
        while current_date <= date(year_to_fetch, 12, 31):

            first_day_of_month = current_date.replace(day=1)
            last_day_of_month = date(year_to_fetch, current_date.month,
                                     calendar.monthrange(year_to_fetch, current_date.month)[1])

            result_prod_data = await get_prod_data(db, first_day_of_month, last_day_of_month)
            result_dur_data = await get_dur_data(db, first_day_of_month, last_day_of_month)
            result_prod_finish = await get_prod_data_finish(db, first_day_of_month, last_day_of_month)
            result_prod_heat = await get_prod_data_heat_set(db, first_day_of_month, last_day_of_month)
            result_dur_finish = await get_dur_finish(db, first_day_of_month, last_day_of_month)
            result_dur_heat = await get_dur_heat_set(db, first_day_of_month, last_day_of_month)
            result_data_for_month = {}
            for machine, production in result_prod_data.items():
                result_data_for_month[machine] = {
                    'production': production,
                    'duration_hours': result_dur_data.get(machine, 0),
                    'production_finish': result_prod_finish.get(machine, 0),
                    'production_heat_set': result_prod_heat.get(machine, 0),
                    'duration_finish': result_dur_finish.get(machine, 0),
                    'duration_heat': result_dur_heat.get(machine, 0)
                }

            result_data_for_year[current_date.strftime('%B')] = result_data_for_month

            current_date = last_day_of_month + timedelta(days=1)

        return result_data_for_year

    except Exception as e:
        print(e)


async def get_prod_data(db: Session, start_date: date, end_date: date) -> Dict[str, Dict[str, int]]:
    try:
        query = db.query(

            models.RunData.machine,
            sqlalchemy.func.sum(models.RunData.meters).label('production'),
        ).filter(
            models.RunData.date_ >= start_date,
            models.RunData.date_ <= end_date
        ).group_by(models.RunData.machine)

        data = query.all()

        result_data = {}

        for machine, production in data:

            if machine not in result_data:
                result_data[machine] = {}

            result_data[machine] = int(production)

        return result_data

    except Exception as e:
        print(e)


async def get_prod_data_finish(db: Session, start_date: date, end_date: date) -> Dict[str, Dict[str, int]]:
    try:
        query = db.query(
            models.RunData.machine,
            # models.RunData.operation_name,
            sqlalchemy.func.sum(models.RunData.meters).label('production'),
        ).filter(
            models.RunData.date_ >= start_date,
            models.RunData.date_ <= end_date,
            models.RunData.operation_name == 'FINISHNG'
        ).group_by(models.RunData.machine)

        data = query.all()

        result_data = {}

        for machine, production in data:

            if machine not in result_data:
                result_data[machine] = {}

            result_data[machine] = int(production)

        return result_data

    except Exception as e:
        print(e)


async def get_prod_data_heat_set(db: Session, start_date: date, end_date: date) -> Dict[str, Dict[str, int]]:
    try:
        query = db.query(
            models.RunData.machine,

            sqlalchemy.func.sum(models.RunData.meters).label('production'),
        ).filter(
            models.RunData.date_ >= start_date,
            models.RunData.date_ <= end_date,
            models.RunData.operation_name == 'HEATSETT'

        ).group_by(models.RunData.machine)

        data = query.all()

        result_data = {}

        for machine, production in data:

            if machine not in result_data:
                result_data[machine] = {}

            result_data[machine] = int(production)

        return result_data

    except Exception as e:
        print(e)


async def get_dur_data(db: Session, start_date: date, end_date: date) -> Dict[str, Dict[str, int]]:
    try:
        query = db.query(

            models.RunData.machine,
            cast(sqlalchemy.func.sum(models.RunData.duration) / 3600.0, Float).label('production_hours')
        ).filter(
            models.RunData.date_ >= start_date,
            models.RunData.date_ <= end_date
        ).group_by(models.RunData.machine)

        data = query.all()

        result_data = {}

        for machine, production in data:

            if machine not in result_data:
                result_data[machine] = {}

            result_data[machine] = round(production, 2)

        return result_data

    except Exception as e:
        print(e)


async def get_dur_finish(db: Session, start_date: date, end_date: date) -> Dict[str, Dict[str, int]]:
    try:
        query = db.query(

            models.RunData.machine,
            cast(sqlalchemy.func.sum(models.RunData.duration) / 3600.0, Float).label('production_hours')
        ).filter(
            models.RunData.date_ >= start_date,
            models.RunData.date_ <= end_date,
            models.RunData.operation_name == 'FINISHNG'
        ).group_by(models.RunData.machine)

        data = query.all()

        result_data = {}

        for machine, production in data:

            if machine not in result_data:
                result_data[machine] = {}

            result_data[machine] = round(production, 2)

        return result_data

    except Exception as e:
        print(e)


async def get_dur_heat_set(db: Session, start_date: date, end_date: date) -> Dict[str, Dict[str, int]]:
    try:
        query = db.query(

            models.RunData.machine,
            cast(sqlalchemy.func.sum(models.RunData.duration) / 3600.0, Float).label('production_hours')
        ).filter(
            models.RunData.date_ >= start_date,
            models.RunData.date_ <= end_date,
            models.RunData.operation_name == 'HEATSETT'
        ).group_by(models.RunData.machine)

        data = query.all()

        result_data = {}

        for machine, production in data:

            if machine not in result_data:
                result_data[machine] = {}

            result_data[machine] = round(production, 2)

        return result_data

    except Exception as e:
        print(e)


# .....................................................................................................
async def create_po_data(db: Session, podata_list: schemas.PoDataBase):
    db_podata_list = models.PoData(po_id=podata_list.po_id, po_number=podata_list.po_number,
                                   article=podata_list.article, greige_glm=podata_list.greige_glm,
                                   finish_glm=podata_list.finish_glm, construction=podata_list.construction,
                                   hmi_data=podata_list.hmi_data, machine=podata_list.machine,
                                   plant_name=podata_list.plant_name)

    db.add(db_podata_list)
    db.commit()
    db.refresh(db_podata_list)
    return db_podata_list


async def get_po_number(db: Session, machine: str, po_number: str, plant_name: str):
    try:
        return db.query(models.PoData).filter(models.PoData.machine == machine,
                                              models.PoData.po_number == po_number,
                                              models.PoData.plant_name == plant_name
                                              ).first()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{e}")


async def update_po_data(db: Session, data: schemas.PoDataBase):
    try:
        data_db = await get_po_number(db, data.machine, data.po_number, data.plant_name)

        mo_id = data_db.id
        db_mo = db.get(models.PoData, mo_id)
        for key, value in data.dict(exclude_unset=True).items():
            setattr(db_mo, key, value)
        db.add(db_mo)
        db.commit()

        db.refresh(db_mo)
        return db_mo
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{e}")


async def get_monthly_production(db: Session, date_: date):
    try:
        month = date_.month
        current_year = date_.year
        first_day_of_month = datetime.date(datetime(current_year, month, 1))
        last_day_of_month = first_day_of_month.replace(day=calendar.monthrange(current_year, month)[1])
        date_from = first_day_of_month.strftime("%Y-%m-%d")
        date_to = last_day_of_month.strftime("%Y-%m-%d")
        month_name = first_day_of_month.strftime("%B")
        date_range = pd.date_range(date_from, date_to, freq='D')
        all_machines = ['Stenter-1', 'Stenter-2', 'Stenter-3', 'Stenter-4', 'Stenter-5', 'Stenter-6',
                        'Relax-Drier', 'Sanforiser-2', 'Sanforiser-3', 'Sanforiser-4', 'Curing',
                        'Airo-24']
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
        print(e)


async def get_airo_production(db: Session, date_: date):
    global machines_in_data, date_data
    try:
        month = date_.month
        current_year = date_.year
        first_day_of_month = datetime.date(datetime(current_year, month, 1))
        last_day_of_month = first_day_of_month.replace(day=calendar.monthrange(current_year, month)[1])
        date_from = first_day_of_month.strftime("%Y-%m-%d")
        date_to = last_day_of_month.strftime("%Y-%m-%d")

        date_range = pd.date_range(date_from, date_to, freq='D')

        combined_data = []

        for date_data in date_range:
            non_eff_data = db.query(
                models.RunData.machine,
                sqlalchemy.func.sum(models.RunData.meters).label('production')
            ).filter(
                models.RunData.date_ == date_data,
                models.RunData.machine == 'Airo-24',  # Airo-24
                models.RunData.operation_name == 'DRYAIRO',
                models.RunData.run_category == 'Reprocess'
            ).group_by(models.RunData.machine)

            total_data = db.query(
                models.RunData.machine,
                sqlalchemy.func.sum(models.RunData.meters).label('production')
            ).filter(
                models.RunData.date_ == date_data,
                models.RunData.machine == 'Airo-24',
                models.RunData.operation_name == 'DRYAIRO',
                # ~models.RunData.run_category.in_(non_effective_category)        # ~ not in
            ).group_by(models.RunData.machine).all()

            non_eff_production = sum(production for _, production in non_eff_data) if non_eff_data else 0
            total_production = sum(production for _, production in total_data) if total_data else 0

            combined_entry = {
                "date": date_data.strftime('%Y-%m-%d'),
                "machine": 'Airo-24',
                "non_effective_production": non_eff_production,
                "production_total": total_production
            }

            combined_data.append(combined_entry)

        return combined_data

    except Exception as e:
        print(e)


##............................HEATSET Speed and Temp..........................................


# .............................................................................................
async def get_reprocess_and_process_demand(db: Session, date_: date):
    global production_query_data
    run_categories_yd = ['Yarn dyed', 'Process demand']
    run_categories_pd = ['Piece dyed', 'Process demand']
    all_stenter_machines = ['Stenter-1', 'Stenter-2', 'Stenter-3', 'Stenter-4', 'Stenter-5', 'Stenter-6']
    sanfo_machines = ['Sanforiser-2', 'Sanforise-3', 'Sanforiser-4']
    machine_mapping = {
        'Sanforiser-2': 'FSN00102',
        'Sanforise-3': 'FSN00103',
        'Sanforiser-4': 'FSN00104'
    }

    try:
        first_date_of_month = date_.replace(day=1)

        reprocess_data = await get_reprocess_data_of_all_stenter_machines(db, date_)

        process_demand_data = await get_process_demand_data_of_all_stenter_machines(db, date_)

        relax_dryer_data = await get_relax_dryer_heat_set_data(db, date_)

        sanfo_reprocess_data = await get_sanfo_reprocess_data(db, date_)

        sanfo_process_demand_data = await get_sanfo_process_demand_data(db, date_)

        total_stenter_production_ftd = await calculate_total_production(db, date_, date_, all_stenter_machines)
        total_stenter_production_mtd = await calculate_total_production(db, first_date_of_month, date_,
                                                                        all_stenter_machines)

        total_stenter_reprocess_ftd = await calculate_total_production_category(db, date_, date_, all_stenter_machines,
                                                                                'Reprocess')
        total_stenter_reprocess_mtd = await calculate_total_production_category(db, first_date_of_month, date_,
                                                                                all_stenter_machines, 'Reprocess')

        total_stenter_process_demand_ftd = await calculate_total_production_category(db, date_, date_,
                                                                                     all_stenter_machines,
                                                                                     'Process demand')
        total_stenter_process_demand_mtd = await calculate_total_production_category(db, first_date_of_month, date_,
                                                                                     all_stenter_machines,
                                                                                     'Process demand')

        ################################## SANFORISER MACHINES
        total_sanfo_production_ftd = await calculate_total_production(db, date_, date_, sanfo_machines)
        total_sanfo_production_mtd = await calculate_total_production(db, first_date_of_month, date_, sanfo_machines)

        total_sanfo_reprocess_ftd = await calculate_total_production_category(db, date_, date_, sanfo_machines,
                                                                              'Reprocess')
        total_sanfo_reprocess_mtd = await calculate_total_production_category(db, first_date_of_month, date_,
                                                                              sanfo_machines, 'Reprocess')

        total_sanfo_process_demand_ftd = await calculate_total_production_category(db, date_, date_, sanfo_machines,
                                                                                   'Process demand')
        total_sanfo_process_demand_mtd = await calculate_total_production_category(db, first_date_of_month, date_,
                                                                                   sanfo_machines, 'Process demand')

        def get_effective_production(db, start_date, end_date, machine):
            query = db.query(
                sqlalchemy.func.sum(models.RunData.meters).label('production')
            ).filter(
                models.RunData.date_.between(start_date, end_date),
                models.RunData.machine.in_(machine),
                models.RunData.run_category != 'Reprocess'

            )

            total_data = query.all()
            return total_data[0][0] if total_data and total_data[0][0] is not None else 0

        sanfo_effective_mtd = get_effective_production(db, first_date_of_month, date_, sanfo_machines)

        stenter_effective_mtd = get_effective_production(db, first_date_of_month, date_, all_stenter_machines)

        curing_effective_ftd = get_effective_production(db, date_, date_, ['Curing'])
        curing_effective_mtd = get_effective_production(db, first_date_of_month, date_, ['Curing'])

        def get_curing_heatsett_production(db, start_date, end_date, machine):
            query = db.query(
                sqlalchemy.func.sum(models.RunData.meters).label('production')
            ).filter(
                models.RunData.date_.between(start_date, end_date),
                models.RunData.machine.in_(machine),
                models.RunData.run_category == 'HEATSETT'

            )

            total_data = query.all()
            return total_data[0][0] if total_data and total_data[0][0] is not None else 0

        curing_heatsett_ftd = get_curing_heatsett_production(db, date_, date_, ['Curing'])
        curing_heatsett_mtd = get_curing_heatsett_production(db, first_date_of_month, date_, ['Curing'])

        function_data = {"reprocess_data": reprocess_data,
                         "process_demand_data": process_demand_data,
                         "relax_dryer_data": relax_dryer_data,
                         "sanfo_reprocess_data": sanfo_reprocess_data,
                         "sanfo_process_demand_data": sanfo_process_demand_data,
                         "total_stenter_production_ftd": int(total_stenter_production_ftd),
                         "total_stenter_production_mtd": int(total_stenter_production_mtd),
                         "total_stenter_reprocess_ftd": int(total_stenter_reprocess_ftd),
                         "total_stenter_reprocess_mtd": int(total_stenter_reprocess_mtd),
                         "total_stenter_process_demand_ftd": int(total_stenter_process_demand_ftd),
                         "total_stenter_process_demand_mtd": int(total_stenter_process_demand_mtd),
                         "total_sanfo_production_ftd": int(total_sanfo_production_ftd),
                         "total_sanfo_production_mtd": int(total_sanfo_production_mtd),
                         "total_sanfo_reprocess_ftd": int(total_sanfo_reprocess_ftd),
                         "total_sanfo_reprocess_mtd": int(total_sanfo_reprocess_mtd),
                         "total_sanfo_process_demand_ftd": int(total_sanfo_process_demand_ftd),
                         "total_sanfo_process_demand_mtd": int(total_sanfo_process_demand_mtd),
                         "sanfo_effective_mtd": int(sanfo_effective_mtd),
                         "stenter_effective_mtd": int(stenter_effective_mtd),
                         "curing_effective_ftd": int(curing_effective_ftd),
                         "curing_effective_mtd": int(curing_effective_mtd),
                         "curing_heatsett_ftd": int(curing_heatsett_ftd),
                         "curing_heatsett_mtd": int(curing_heatsett_mtd)

                         }

        return function_data

    except Exception as e:
        return HTTPException(status_code=500, detail=f"{e}")


async def get_airo_effective_noneffective_data(db: Session, date_: date):
    global machine_name, machine_data
    try:
        first_date_of_month = date_.replace(day=1)
        non_effective_run_categories = ['REPROCESS']

        def get_airo_total_production(db, start_date, end_date, machine):
            query = db.query(
                sqlalchemy.func.sum(models.RunData.meters).label('production')
            ).filter(
                models.RunData.date_.between(start_date, end_date),
                models.RunData.machine == machine,
                models.RunData.operation_name != 'AIROBEAT'
            )

            total_data = query.all()
            return total_data[0][0] if total_data and total_data[0][0] is not None else 0

        total_production_ftd = get_airo_total_production(db, date_, date_, 'Airo-24')
        total_production_mtd = get_airo_total_production(db, first_date_of_month, date_, 'Airo-24')

        def get_dryairo_total_production(db, start_date, end_date, machine, operation_name):
            query = db.query(
                sqlalchemy.func.sum(models.RunData.meters).label('production')
            ).filter(
                models.RunData.date_.between(start_date, end_date),
                models.RunData.machine == machine,
                models.RunData.operation_name == operation_name,
                models.RunData.run_category != 'Reprocess'
            )

            total_data = query.all()
            return total_data[0][0] if total_data and total_data[0][0] is not None else 0

        total_dryairo_ftd = get_dryairo_total_production(db, date_, date_, 'Airo-24', 'DRYAIRO')

        total_dryairo_mtd = get_dryairo_total_production(db, first_date_of_month, date_, 'Airo-24', 'DRYAIRO')
        print("total_dryairo_mtd:->", round(total_dryairo_mtd))
        total_airobeat_ftd = get_dryairo_total_production(db, date_, date_, 'Airo-24', 'AIROBEAT')
        total_airobeat_mtd = get_dryairo_total_production(db, first_date_of_month, date_, 'Airo-24', 'AIROBEAT')

        def get_airo_total_production_effective(db, start_date, end_date, machine, non_effective_run_categories):
            query = db.query(
                sqlalchemy.func.sum(models.RunData.meters).label('production')
            ).filter(
                models.RunData.date_.between(start_date, end_date),
                models.RunData.machine == machine,
                ~models.RunData.run_category.in_(non_effective_run_categories)

            )

            total_data = query.all()
            return total_data[0][0] if total_data and total_data[0][0] is not None else 0

        total_production_effective_ftd = get_airo_total_production_effective(db, date_,
                                                                             date_,
                                                                             'Airo-24', ['Reprocess'])

        total_production_effective_mtd = get_airo_total_production_effective(db, first_date_of_month, date_,
                                                                             'Airo-24',
                                                                             ['Reprocess'])

        def get_airo_total_production_non_effective(db, start_date, end_date, machine, non_effective_run_categories):
            query = db.query(
                sqlalchemy.func.sum(models.RunData.meters).label('production')
            ).filter(
                models.RunData.date_.between(start_date, end_date),
                models.RunData.machine == machine,
                models.RunData.run_category.in_(non_effective_run_categories)
            )

            total_data = query.all()
            return total_data[0][0] if total_data and total_data[0][0] is not None else 0

        total_production_non_effective_ftd = get_airo_total_production_non_effective(db, date_,
                                                                                     date_, 'Airo-24',
                                                                                     ['Reprocess'])

        total_production_non_effective_mtd = get_airo_total_production_non_effective(db, first_date_of_month, date_,
                                                                                     'Airo-24',
                                                                                     ['Reprocess'])

        def get_dryairo_total_noneffective_production(db, start_date, end_date, machine):
            query = db.query(
                sqlalchemy.func.sum(models.RunData.meters).label('production')
            ).filter(
                models.RunData.date_.between(start_date, end_date),
                models.RunData.machine == machine,
                models.RunData.operation_name == 'DRYAIRO',
                models.RunData.run_category == 'Reprocess'
                # models.RunData.operation_name.in_(['DRYAIRO', 'REPROCESS'])
            ).group_by(
                models.RunData.machine
            )

            total_data = query.all()
            return total_data[0][0] if total_data and total_data[0][0] is not None else 0

        dry_airo_production_noneffective_ftd = get_dryairo_total_noneffective_production(db, date_, date_, 'Airo-24')

        dry_airo_production_noneffective_mtd = get_dryairo_total_noneffective_production(db, first_date_of_month, date_,
                                                                                         'Airo-24')

        airo_beat_production_effective_ftd = get_dryairo_total_noneffective_production(db, date_,
                                                                                       date_,
                                                                                       'Airo-24')

        airo_beat_production_effective_mtd = get_dryairo_total_noneffective_production(db, first_date_of_month, date_,
                                                                                       'Airo-24')

        airo_non_eff_data = await get_airo_non_effective_po_data(db, date_)

        query = db.query(
            models.RunData.machine,
            models.RunData.shift,
            cast(sqlalchemy.func.sum(models.RunData.duration) / 3600.0, Float).label('production_hours')
        ).filter(
            models.RunData.date_ == date_,
            models.RunData.machine == 'Airo-24'
        ).group_by(models.RunData.machine, models.RunData.shift)

        result = query.all()
        running_hours = []

        if result:
            machine_data = {"date": date_, "machine": result[0][0]}
            for row in result:
                _, shift, running_hours_value = row
                # machine_data[shift] = float(running_hours_value)
                machine_data[shift] = round(running_hours_value)
        else:
            print("No data for running hours")
            machine_data = {"date": date_, "machine": "Airo-24", "A": 0, "B": 0, "C": 0}

        running_hours.append(machine_data)

        categories = ['Mechanical Breakdown', 'Electrical Breakdown', 'Machine cleaning', 'Fabric changeover',
                      'Preventive Maintenance', 'No Program']

        breakdown_hours_data = []
        for category in categories:
            query = db.query(
                models.StopData.machine,
                cast(sqlalchemy.func.sum(models.StopData.duration) / 3600.0, Float).label(f'{category}_hours'),
            ).filter(
                models.StopData.stop_category == category,
                models.StopData.date_ == date_,
                models.StopData.machine == 'Airo-24'
            ).group_by(models.StopData.machine)
            data = query.all()
            print("data:->", data)
            if data:
                machine_name = data[0][0]
                category_data = data[0][1] if data[0][1] is not None else 0
                breakdown_hours_data.append(int(category_data))
            else:
                machine_name = ''
                breakdown_hours_data.append(0)

        breakdown_hours_list = breakdown_hours_data
        print("breakdown_hours_list:->", breakdown_hours_list)
        breakdown_hours = [{"date": date_,
                            "machine": 'Airo-24',
                            "mechanical_data": breakdown_hours_list[0],
                            "electrical_data": breakdown_hours_list[1],
                            "cleaning_data": breakdown_hours_list[2],
                            "changeover_data": breakdown_hours_list[3],
                            "pm_data": breakdown_hours_list[4],
                            "no_program_data": breakdown_hours_list[5]}]

        return {"airo_total_production_ftd": int(total_production_ftd),
                "airo_total_production_mtd": int(total_production_mtd),
                "dry_airo_ftd": int(total_dryairo_ftd),
                "dry_airo_mtd": int(total_dryairo_mtd),
                "airo_beat_ftd": 0,
                "airo_beat_mtd": 0,
                "airo_total_effective_production_ftd": int(total_production_effective_ftd),
                "airo_total_effective_production_mtd": int(total_production_effective_mtd),
                "airo_total_production_non_effective_ftd": int(total_production_non_effective_ftd),
                "airo_total_production_non_effective_mtd": int(total_production_non_effective_mtd),
                "dry_airo_production_noneffective_ftd": int(dry_airo_production_noneffective_ftd),
                "dry_airo_production_noneffective_mtd": int(dry_airo_production_noneffective_mtd),
                "airo_beat_effective_production_ftd": 0,
                "airo_beat_effective_production_mtd": 0,
                "airo_non_eff_data": airo_non_eff_data,
                "airo_running_hours": running_hours,
                "airo_breakdown_hours": breakdown_hours
                }


    except Exception as e:
        return HTTPException(status_code=500, detail=f"{e}")


async def get_heat_consumption(db: Session, date_: date):
    global machines_in_data, date_data, unique_po_numbers, result_dict_po_numbers_machine, avg_glm_contribution, glm_data
    try:
        month = date_.month
        current_year = date_.year
        first_day_of_month = datetime.date(datetime(current_year, month, 1))
        last_day_of_month = first_day_of_month.replace(day=calendar.monthrange(current_year, month)[1])
        date_from = first_day_of_month.strftime("%Y-%m-%d")
        date_to = last_day_of_month.strftime("%Y-%m-%d")
        date_range = pd.date_range(date_from, date_to, freq='D')

        all_stenter_machines = ['Stenter-1', 'Stenter-2', 'Stenter-3', 'Stenter-4', 'Stenter-5', 'Stenter-6']
        all_total_machines = ['Stenter-1', 'Stenter-2', 'Stenter-3', 'Stenter-4', 'Stenter-5', 'Stenter-6',
                              'Relax-Drier', 'Sanforiser-2', 'Sanforiser-3', 'Sanforiser-4', 'Curing',
                              'Airo-24']

        combined_data = []
        for date_data in date_range:
            machine_data = db.query(
                models.RunData.machine,
                sqlalchemy.func.sum(models.RunData.meters).label('production')
            ).filter(
                models.RunData.date_ == date_data,
                models.RunData.machine.in_(all_stenter_machines)
            ).group_by(models.RunData.machine)
            data = machine_data.all()
            data_production_stenters = sum(production for _, production in data) if data else 0

            machine_heat_run_data = db.query(models.RunData.machine,
                                             sqlalchemy.func.sum(models.RunData.fluid_total).label(
                                                 'fluid_total')).filter(
                models.RunData.date_ == date_data,
                models.RunData.machine.in_(all_stenter_machines)
            ).group_by(models.RunData.machine, models.RunData.date_)
            machine_heat_data = machine_heat_run_data.all()
            machine_run_data_sum = sum(production for _, production in machine_heat_data) if machine_heat_data else 0

            machine_heat_stop_data = db.query(models.StopData.machine,
                                              sqlalchemy.func.sum(models.StopData.fluid_total).label(
                                                  'fluid_total')).filter(
                models.StopData.date_ == date_data,
                models.StopData.machine.in_(all_stenter_machines)
            ).group_by(models.StopData.machine, models.StopData.date_)
            machine_stop_data = machine_heat_stop_data.all()
            machine_stop_data_sum = sum(production for _, production in machine_stop_data) if machine_stop_data else 0

            total_heat = machine_run_data_sum + machine_stop_data_sum
            total_heat_thermic = total_heat / 10

            thermic_heat_run_data = db.query(models.RunData.machine,
                                             sqlalchemy.func.sum(models.RunData.fluid_total).label(
                                                 'fluid_total')).filter(
                models.RunData.date_ == date_data,
                models.RunData.machine == 'Thermofix'
            ).group_by(models.RunData.machine, models.RunData.date_)
            thermic_heat_data = thermic_heat_run_data.all()
            thermic_run_data_sum = sum(production for _, production in thermic_heat_data) if thermic_heat_data else 0

            thermic_heat_stop_data = db.query(models.StopData.machine,
                                              sqlalchemy.func.sum(models.StopData.fluid_total).label(
                                                  'fluid_total')).filter(
                models.StopData.date_ == date_data,
                models.StopData.machine == 'Thermofix'
            ).group_by(models.StopData.machine, models.StopData.date_)
            thermic_stop_data = thermic_heat_stop_data.all()
            thermic_stop_data_sum = sum(production for _, production in thermic_stop_data) if thermic_stop_data else 0

            total_thermic_heat = thermic_run_data_sum + thermic_stop_data_sum
            heat_thermic_fluid = total_thermic_heat / 10

            thermofix_data = db.query(models.RunData.machine,
                                      sqlalchemy.func.sum(models.RunData.meters).label(
                                          'production')).filter(
                models.RunData.date_ == date_data,
                models.RunData.operation_name == 'THERMFIX',
                models.RunData.machine.in_(['Stenter-3', 'Stenter-4'])
            ).group_by(models.RunData.machine)
            machine_thermofix_data = thermofix_data.all()
            thermofix_production = sum(
                production for _, production in machine_thermofix_data) if machine_thermofix_data else 0

            ################### CALCULATE AVG_GLM
            machine_production = data

            unique_po_numbers = db.query(

                models.RunData.machine,
                models.RunData.po_number
            ).filter(
                models.RunData.date_ == date_data,
                models.RunData.machine.in_(all_stenter_machines)
            ).group_by(models.RunData.machine, models.RunData.po_number)

            po_data_query = unique_po_numbers.all()

            po_machines = [item[0] for item in po_data_query]
            unique_po_numbers = [item[1] for item in po_data_query]
            unique_po_count = len(set(unique_po_numbers))
            print("Unique PO count:", unique_po_count)
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

            print("aggregated_glm_data-->", aggregated_glm_data)
            total_greige_glm_sum = sum(aggregated_glm_data.values())
            # print("total_greige_glm_sum-->",total_greige_glm_sum)

            # Calculate the average greige_glm
            avg_glm = total_greige_glm_sum / unique_po_count if unique_po_count != 0 else 0

            # print("Average GLM:", avg_glm)

            combined_entry = {
                "date": date_data.strftime('%Y-%m-%d'),
                "stenter_data_production": int(data_production_stenters),
                "avg_glm": int(avg_glm),
                "total_heat_thermic": int(total_heat_thermic),
                "thermofix_production": int(thermofix_production),
                "heat_thermic_fluid": int(heat_thermic_fluid)
            }

            combined_data.append(combined_entry)

        return combined_data

    except Exception as e:
        print(e)


async def get_airo_non_effective_po_data(db: Session, date_: date):
    global meters_production
    try:
        airo_unique_po_numbers_reprocess = db.query(models.RunData.po_number).filter(
            models.RunData.date_ == date_,
            models.RunData.machine == 'Airo-24',  # Airo-24
            models.RunData.operation_name == 'DRYAIRO',
            models.RunData.run_category == 'Reprocess'

        ).distinct().all()

        airo_non_eff_data = []
        if not airo_unique_po_numbers_reprocess:
            airo_non_eff_data = []

        if airo_unique_po_numbers_reprocess:
            airo_unique_po_numbers_reprocess_list = [item[0] for item in airo_unique_po_numbers_reprocess]

            for po_num in airo_unique_po_numbers_reprocess_list:
                run_data = db.query(models.RunData.po_number, sqlalchemy.func.sum(models.RunData.meters).label(
                    'production')).filter(models.RunData.date_ == date_,
                                          models.RunData.machine == 'Airo-24',
                                          models.RunData.operation_name == 'DRYAIRO',
                                          models.RunData.run_category == 'Reprocess',
                                          models.RunData.po_number == po_num).group_by(models.RunData.po_number).first()

                po_data = db.query(models.PoData.po_number,
                                   models.PoData.article).filter(
                    models.PoData.po_number == po_num).first()

                if run_data:
                    po_number = run_data[0]
                    # meters_production = run_data[1] if run_data[1] else 0
                    meters_production = round(run_data[1], 2) if run_data[1] else 0

                    # Extract article string from PoData and split into parts
                    if po_data and po_data[1]:
                        article_string = po_data[1]
                        article_parts = article_string.split(',')
                        keys = ['k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7', 'k8', 'k9']
                        article_dict_non_effective = {key: value for key, value in zip(keys, article_parts)}
                        finish_code_value = article_dict_non_effective.get('k3', '')

                        # Construct entry for airo_non_eff_data
                        airo_non_eff_data.append({
                            "date": date_,
                            "article": article_string,
                            "po_number": po_num,
                            "meter_production": meters_production,
                            "finish_Code": finish_code_value,
                            "detail": "Non Route"
                        })
                        print("airo_non_eff_data:", airo_non_eff_data)

                    else:
                        print(f"No data found in PoData for po_number {po_num}")
                        # Handle case where PoData does not have matching po_number
                        airo_non_eff_data.append({
                            "date": date_,
                            "article": " ",
                            "po_number": po_num,
                            "meter_production": meters_production,
                            "finish_Code": " ",
                            "detail": " "
                        })

                else:
                    print(f"No data found in RunData for po_number {po_num}")
                    # Handle case where RunData does not have matching po_number
                    airo_non_eff_data.append({
                        "date": date_,
                        "article": " ",
                        "po_number": po_num,
                        "meter_production": meters_production,
                        "finish_Code": " ",
                        "detail": " "
                    })

            print("Final airo_non_eff_data:", airo_non_eff_data)

            return airo_non_eff_data

        else:
            airo_non_eff_data.append({
                "date_": " ",
                "s_no": " ",
                'article': " ",
                'po_number': " ",
                'k3': " ",
                'meter_production': " "
            })
            return airo_non_eff_data
    except Exception as e:
        print(e)


async def get_reprocess_data_of_all_stenter_machines(db: Session, date_: date):
    global meters_production
    try:
        all_stenter_machines = ['Stenter-1', 'Stenter-2', 'Stenter-3', 'Stenter-4', 'Stenter-5', 'Stenter-6']

        stenter_reprocess_unique_po_number = db.query(models.RunData.po_number).filter(
            models.RunData.date_ == date_,
            models.RunData.machine.in_(all_stenter_machines),
            models.RunData.run_category == 'Reprocess'
        ).distinct().all()

        stenter_reprocess_data = []
        if stenter_reprocess_unique_po_number:
            stenter_reprocess_unique_po_number_list = [item[0] for item in stenter_reprocess_unique_po_number]

            serial_number = 1
            for po_num in stenter_reprocess_unique_po_number_list:

                run_data = db.query(models.RunData.po_number, sqlalchemy.func.sum(models.RunData.meters).label(
                    'production')).filter(models.RunData.machine.in_(all_stenter_machines),
                                          models.RunData.date_ == date_,
                                          models.RunData.po_number == po_num,
                                          models.RunData.run_category == 'Reprocess'
                                          ).group_by(models.RunData.po_number).first()

                po_data = db.query(models.PoData.po_number,
                                   models.PoData.article).filter(models.PoData.po_number == po_num).first()

                if run_data:
                    po_number = run_data[0]
                    meters_production = int(run_data[1]) if run_data[1] else 0

                    if po_data and po_data[1]:
                        article_string = po_data[1]
                        article_parts = article_string.split(',')
                        keys = ['k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7', 'k8', 'k9']
                        article_dict = {key: value for key, value in zip(keys, article_parts)}

                        stenter_reprocess_data.append({
                            "date_": date_,
                            "s_no": serial_number,
                            'article': article_string,
                            'po_number': po_number,
                            'k3': article_dict.get('k3', ''),
                            'meter_production': meters_production
                        })


                    else:
                        print(f"No data found in PoData for po_number {po_num}")
                        # Handle case where PoData does not have matching po_number
                        stenter_reprocess_data.append({
                            "date_": date_,
                            "s_no": serial_number,
                            'article': " ",
                            'po_number': po_number,
                            'k3': " ",
                            'meter_production': meters_production
                        })

                else:
                    print(f"No data found in RunData for po_number {po_num}")
                    # Handle case where RunData does not have matching po_number
                    stenter_reprocess_data.append({
                        "date_": date_,
                        "s_no": serial_number,
                        'article': " ",
                        'po_number': po_num,
                        'k3': " ",
                        'meter_production': meters_production
                    })

                serial_number += 1  # Increment serial number for next iteration

            return stenter_reprocess_data

        else:
            stenter_reprocess_data.append({
                "date_": " ",
                "s_no": " ",
                'article': " ",
                'po_number': " ",
                'k3': " ",
                'meter_production': " "
            })
            return stenter_reprocess_data
    except Exception as e:
        print(e)


async def get_process_demand_data_of_all_stenter_machines(db: Session, date_: date):
    global meters_production
    try:
        all_stenter_machines = ['Stenter-1', 'Stenter-2', 'Stenter-3', 'Stenter-4', 'Stenter-5', 'Stenter-6']

        stenter_process_demand_unique_po_number = db.query(models.RunData.po_number).filter(
            models.RunData.date_ == date_,
            models.RunData.machine.in_(all_stenter_machines),
            models.RunData.run_category == 'Process demand'
        ).distinct().all()

        stenter_process_demand_demand_data = []
        if stenter_process_demand_unique_po_number:
            stenter_process_demand_unique_po_number_list = [item[0] for item in stenter_process_demand_unique_po_number]

            serial_number = 1
            for po_num in stenter_process_demand_unique_po_number_list:
                run_data = db.query(models.RunData.po_number, sqlalchemy.func.sum(models.RunData.meters).label(
                    'production')).filter(models.RunData.machine.in_(all_stenter_machines),
                                          models.RunData.date_ == date_,
                                          models.RunData.po_number == po_num,
                                          models.RunData.run_category == 'Process demand'
                                          ).group_by(models.RunData.po_number).first()

                po_data = db.query(models.PoData.po_number,
                                   models.PoData.article).filter(models.PoData.po_number == po_num).first()

                if run_data:
                    po_number = run_data[0]
                    meters_production = int(run_data[1]) if run_data[1] else 0

                    # Extract article string from PoData and split into parts
                    if po_data and po_data[1]:
                        article_string = po_data[1]
                        article_parts = article_string.split(',')
                        keys = ['k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7', 'k8', 'k9']
                        article_dict = {key: value for key, value in zip(keys, article_parts)}

                        stenter_process_demand_demand_data.append({
                            "date_": date_,
                            "s_no": serial_number,
                            'article': article_string,
                            'po_number': po_number,
                            'k3': article_dict.get('k3', ''),
                            'meter_production': meters_production
                        })


                    else:
                        print(f"No data found in PoData for po_number {po_num}")
                        # Handle case where PoData does not have matching po_number
                        stenter_process_demand_demand_data.append({
                            "date_": date_,
                            "s_no": serial_number,
                            'article': 0,
                            'po_number': po_number,
                            'k3': 0,
                            'meter_production': meters_production
                        })

                else:
                    print(f"No data found in RunData for po_number {po_num}")
                    # Handle case where RunData does not have matching po_number
                    stenter_process_demand_demand_data.append({
                        "date_": date_,
                        "s_no": serial_number,
                        'article': 0,
                        'po_number': po_num,
                        'k3': 0,
                        'meter_production': meters_production
                    })

                serial_number += 1  # Increment serial number for next iteration

            return stenter_process_demand_demand_data

    except Exception as e:
        print(e)


async def get_relax_dryer_heat_set_data(db: Session, date_: date):
    global meters_production
    try:

        unique_po_numbers_relax_dryer = db.query(models.RunData.po_number).filter(
            models.RunData.date_ == date_,
            models.RunData.machine == 'Relax-Drier',
            models.RunData.operation_name == 'HEATSETT'
        ).distinct().all()

        relax_drier_heat_set_data = []
        if unique_po_numbers_relax_dryer:
            unique_po_numbers_relax_dryer_list = [item[0] for item in unique_po_numbers_relax_dryer]
            serial_number = 1
            for po_num in unique_po_numbers_relax_dryer_list:
                run_data = db.query(models.RunData.po_number, sqlalchemy.func.sum(models.RunData.meters).label(
                    'production')).filter(models.RunData.machine == 'Relax-Drier',
                                          models.RunData.date_ == date_,
                                          models.RunData.po_number == po_num,
                                          models.RunData.operation_name == 'HEATSETT'
                                          ).group_by(models.RunData.po_number).first()

                po_data = db.query(models.PoData.po_number,
                                   models.PoData.article).filter(models.PoData.po_number == po_num).first()

                if run_data:
                    po_number = run_data[0]
                    meters_production = int(run_data[1]) if run_data[1] else 0

                    if po_data and po_data[1]:
                        article_string = po_data[1]
                        article_parts = article_string.split(',')
                        keys = ['k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7', 'k8', 'k9']
                        article_dict = {key: value for key, value in zip(keys, article_parts)}

                        relax_drier_heat_set_data.append({
                            "date_": date_,
                            "s_no": serial_number,
                            'article': article_string,
                            'po_number': po_number,
                            'k3': article_dict.get('k3', ''),
                            'meter_production': meters_production
                        })


                    else:
                        print(f"No data found in PoData for po_number {po_num}")
                        # Handle case where PoData does not have matching po_number
                        relax_drier_heat_set_data.append({
                            "date_": date_,
                            "s_no": serial_number,
                            'article': " ",
                            'po_number': po_number,
                            'k3': " ",
                            'meter_production': meters_production
                        })

                else:
                    print(f"No data found in RunData for po_number {po_num}")
                    relax_drier_heat_set_data.append({
                        "date_": date_,
                        "s_no": serial_number,
                        'article': " ",
                        'po_number': po_num,
                        'k3': " ",
                        'meter_production': meters_production
                    })

                serial_number += 1  # Increment serial number for next iteration
            return relax_drier_heat_set_data

        else:
            relax_drier_heat_set_data.append({
                "date_": " ",
                "s_no": " ",
                'article': " ",
                'po_number': " ",
                'k3': " ",
                'meter_production': " "
            })
            return relax_drier_heat_set_data
    except Exception as e:
        print(e)


async def get_sanfo_reprocess_data(db, date_):
    sanfo_machines = ['Sanforiser-2', 'Sanforise-3', 'Sanforiser-4']
    machine_mapping = {
        'Sanforiser-2': 'FSN00102',
        'Sanforise-3': 'FSN00103',
        'Sanforiser-4': 'FSN00104'
    }
    unique_po_numbers = db.query(models.RunData.po_number).filter(
        models.RunData.date_ == date_,
        models.RunData.machine.in_(sanfo_machines),
        models.RunData.run_category == 'Reprocess',
        # models.RunData.operation_name == operation_name
    ).distinct().all()

    reprocess_data = []
    if unique_po_numbers:
        unique_po_numbers_list = [item[0] for item in unique_po_numbers]
        for po_num in unique_po_numbers_list:
            run_data_query = (
                db.query(
                    models.RunData.machine,
                    models.RunData.po_number,
                    func.count(models.RunData.po_number).label('po_number_count')
                    # func.sum(models.RunData.meters).label('meters_sum')
                )
                .filter(
                    models.RunData.date_ == date_,
                    models.RunData.po_number == po_num,
                    models.RunData.run_category == 'Reprocess',
                    models.RunData.machine.in_(sanfo_machines)
                )
                .group_by(models.RunData.machine, models.RunData.po_number)
            ).first()

            po_data_query = (
                db.query(
                    models.PoData.po_number,
                    models.PoData.article,
                    models.PoData.machine
                )
                .filter(
                    models.PoData.po_number == po_num
                )
            ).first()

            if run_data_query:
                po_number = run_data_query.po_number
                po_qty = int(run_data_query.po_number_count) if run_data_query.po_number_count else 0
                # meters_sum = int(run_data_query.meters_sum) if run_data_query.meters_sum else 0
                machine = run_data_query.machine

                if po_data_query:
                    article_string = po_data_query.article
                    article_parts = article_string.split(',')
                    keys = ['k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7', 'k8', 'k9']
                    article_dict = {key: value for key, value in zip(keys, article_parts)}

                    machine_code = machine_mapping.get(machine, '')

                    reprocess_data.append({
                        "date": date_,
                        "mc_number": machine_code,
                        'article': article_string,
                        'po_number': po_number,
                        'k3': article_dict.get('k3', ''),
                        'po_qty': po_qty
                    })
                else:
                    print(f"No data found in PoData for po_number {po_num}")
                    reprocess_data.append({
                        "date": date_,
                        "mc_number": machine_mapping.get(machine, ''),
                        'article': " ",
                        'po_number': po_number,
                        'k3': " ",
                        'po_qty': po_qty
                    })

            else:
                print(f"No data found in RunData for po_number {po_num}")
                reprocess_data.append({
                    "date": " ",
                    "mc_number": " ",
                    'article': " ",
                    'po_number': " ",
                    'k3': " ",
                    'po_qty': " "
                })
                return reprocess_data

    else:
        reprocess_data.append({
            "date": " ",
            "mc_number": " ",
            'article': " ",
            'po_number': " ",
            'k3': " ",
            'po_qty': " "
        })
    return reprocess_data


async def get_sanfo_process_demand_data(db, date_):
    sanfo_machines = ['Sanforiser-2', 'Sanforise-3', 'Sanforiser-4']
    machine_mapping = {
        'Sanforiser-2': 'FSN00102',
        'Sanforise-3': 'FSN00103',
        'Sanforiser-4': 'FSN00104'
    }
    unique_po_numbers = db.query(models.RunData.po_number).filter(
        models.RunData.date_ == date_,
        models.RunData.machine.in_(sanfo_machines),
        models.RunData.run_category == 'Process demand',
        # models.RunData.operation_name == operation_name
    ).distinct().all()

    Process_demand_data = []
    if unique_po_numbers:
        unique_po_numbers_list = [item[0] for item in unique_po_numbers]
        for po_num in unique_po_numbers_list:
            run_data_query = (
                db.query(
                    models.RunData.machine,
                    models.RunData.po_number,
                    func.count(models.RunData.po_number).label('po_number_count')
                    # func.sum(models.RunData.meters).label('meters_sum')
                )
                .filter(
                    models.RunData.date_ == date_,
                    models.RunData.po_number == po_num,
                    models.RunData.run_category == 'Process demand',
                    models.RunData.machine.in_(sanfo_machines)
                )
                .group_by(models.RunData.machine, models.RunData.po_number)
            ).first()

            po_data_query = (
                db.query(
                    models.PoData.po_number,
                    models.PoData.article,
                    models.PoData.machine
                )
                .filter(
                    models.PoData.po_number == po_num
                )
            ).first()

            if run_data_query:
                po_number = run_data_query.po_number
                po_qty = int(run_data_query.po_number_count) if run_data_query.po_number_count else 0
                # meters_sum = int(run_data_query.meters_sum) if run_data_query.meters_sum else 0
                machine = run_data_query.machine

                if po_data_query:
                    article_string = po_data_query.article
                    article_parts = article_string.split(',')
                    keys = ['k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7', 'k8', 'k9']
                    article_dict = {key: value for key, value in zip(keys, article_parts)}

                    machine_code = machine_mapping.get(machine, '')

                    Process_demand_data.append({
                        "date": date_,
                        "mc_number": machine_code,
                        'article': article_string,
                        'po_number': po_number,
                        'k3': article_dict.get('k3', ''),
                        'po_qty': po_qty
                    })
                else:
                    print(f"No data found in PoData for po_number {po_num}")
                    Process_demand_data.append({
                        "date": date_,
                        "mc_number": machine_mapping.get(machine, ''),
                        'article': " ",
                        'po_number': po_number,
                        'k3': " ",
                        'po_qty': po_qty
                    })

            else:
                print(f"No data found in RunData for po_number {po_num}")
                Process_demand_data.append({
                    "date": " ",
                    "mc_number": " ",
                    'article': " ",
                    'po_number': " ",
                    'k3': " ",
                    'po_qty': " "
                })
                return Process_demand_data

    else:
        Process_demand_data.append({
            "date": " ",
            "mc_number": " ",
            'article': " ",
            'po_number': " ",
            'k3': " ",
            'po_qty': " "
        })
    return Process_demand_data


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


async def calculate_total_production_category(db: Session, date_start, date_end, machines, run_category):
    query = db.query(
        sqlalchemy.func.sum(models.RunData.meters).label('production')
    ).filter(
        models.RunData.date_.between(date_start, date_end),
        models.RunData.machine.in_(machines),
        models.RunData.run_category == run_category
    )
    total_data = query.all()
    if total_data and total_data[0][0] is not None:
        return total_data[0][0]
    else:
        return 0


#################.......................................................................................................................


async def get_po_data(db: Session, date_: date):
    machine_mapping = {
        'Stenter-1': 'FST00101',
        'Stenter-2': 'FST00102',
        'Stenter-3': 'FST00103',
        'Stenter-4': 'FST00104',
        'Stenter-5': 'FST00105',
        'Stenter-6': 'FST00106',
        'Sanforiser-2': 'FSN00102',
        'Sanforiser-3': 'FSN00103',
        'Sanforiser-4': 'FSN00104',
        'Curing': 'FCU00101',
        'Airo-24': 'FAR00401',
        'Thermofix': 'FTF00201',
        'Relax-Drier': 'FRD00101'
    }

    month = date_.month

    all_total_machines = list(machine_mapping.keys())
    unique_po_numbers_rundata = db.query(models.RunData.machine, models.RunData.po_number
                                         ).filter(
        extract('month', models.RunData.date_) == month,
        models.RunData.machine.in_(all_total_machines),
        models.RunData.operation_name == 'HEATSETT'
    ).distinct().subquery()

    unique_data = db.query(unique_po_numbers_rundata).all()

    result_dict = []
    for machine, po_number in unique_data:
        stm = sqlalchemy.select(
            models.PoData.machine,
            models.PoData.po_number,
            models.PoData.article,
            models.PoData.finish_glm,

            func.cast(models.PoData.hmi_data, JSONB).label('hmi_data')

        ).filter(
            models.PoData.machine == machine,
            models.PoData.po_number == po_number
        ).subquery()

        quantity = db.query(stm).all()

        po_counts_subquery = db.query(

            models.RunData.machine,
            models.RunData.po_number,
            func.count(models.RunData.po_number).label('po_qty')
        ).join(
            models.PoData,
            (models.RunData.po_number == models.PoData.po_number) & (
                    models.RunData.machine == models.PoData.machine)
        ).filter(
            extract('month', models.RunData.date_) == month,
            models.RunData.machine == machine,
            models.RunData.po_number == po_number,
            models.RunData.operation_name == 'HEATSETT'

        ).group_by(

            models.RunData.machine,
            models.RunData.po_number
        ).subquery()
        count = db.query(po_counts_subquery).all()
        # print("count", count)

        date_subquery = db.query(

            models.RunData.date_,

        ).join(
            models.PoData,
            (models.RunData.po_number == models.PoData.po_number) & (
                    models.RunData.machine == models.PoData.machine)
        ).filter(
            extract('month', models.RunData.date_) == month,
            models.RunData.machine == machine,
            models.RunData.po_number == po_number,
            models.RunData.operation_name == 'HEATSETT'

        ).subquery()
        date_run = db.query(date_subquery).all()

        meter_subquery = db.query(
            models.RunData.machine,
            models.RunData.po_number,
            func.sum(models.RunData.meters).label('meter')
        ).join(
            models.PoData,
            (models.RunData.po_number == models.PoData.po_number) & (
                    models.RunData.machine == models.PoData.machine)
        ).filter(
            extract('month', models.RunData.date_) == month,
            models.RunData.machine == machine,
            models.RunData.po_number == po_number,
            models.RunData.operation_name == 'HEATSETT'

        ).group_by(

            models.RunData.machine,
            models.RunData.po_number
        ).subquery()
        meter = db.query(meter_subquery).all()
        # print("meter", meter)

        if quantity:
            data = {

                "machine": quantity[0][0],
                "po_number": quantity[0][1],
                "article": quantity[0][2],
                "finish_glm": quantity[0][3],
                "hmi_data": quantity[0][4],
                "po_qty": count[0][2] if count else 0,
                "meter": meter[0][2] if meter else 0,
                "date_run": date_run[0][0]

            }
            result_dict.append(data)

    # Sort result_dict by date_run
    result_dict = sorted(result_dict, key=lambda x: x['date_run'])

    processed_data = []

    for row in result_dict:
        print(row)
        machine, po_number, article, finish_glm, hmi_data, po_qty, meter, date_run = row

        formatted_machine = machine_mapping.get(row['machine'], row['machine'])

        if article is None:
            k1, k2, k3, k4, k5 = [None] * 5
        else:
            article_parts = row['article'].split(',')
            k1 = article_parts[0] if len(article_parts) > 0 else None
            k2 = article_parts[1] if len(article_parts) > 1 else None
            k3 = article_parts[2] if len(article_parts) > 2 else None
            k4 = article_parts[3] if len(article_parts) > 3 else None
            k5 = article_parts[4] if len(article_parts) > 4 else None

        if article is None:
            finish = None
        else:
            finish = f'{k3}'

        if isinstance(row['hmi_data'], str):
            try:
                row['hmi_data'] = json.loads(hmi_data)
            except json.JSONDecodeError:

                row['hmi_data'] = {}
        if isinstance(row['hmi_data'], dict):
            tw_values = row['hmi_data'].get('tw_values', {})
            temperature = tw_values.get('temperature')
            width = tw_values.get('width')
        else:

            temperature = width = None

        row_dict = {
            'po_number': row['po_number'],
            'machine': formatted_machine,
            'date_': row['date_run'],
            'article': row['article'],
            'finish_glm': int(row['finish_glm']),
            'temperature': temperature,
            'width': width,
            'k1': k1,
            'k2': k2,
            'k3': k3,
            'k4': k4,
            'Finish': k3,
            'po_qty': row['po_qty'],
            'meter': int(row['meter'])
        }
        processed_data.append(row_dict)

    return processed_data


async def get_po_data_speed(db: Session, date_: date):
    machine_mapping = {
        'Stenter-1': 'FST00101',
        'Stenter-2': 'FST00102',
        'Stenter-3': 'FST00103',
        'Stenter-4': 'FST00104',
        'Stenter-5': 'FST00105',
        'Stenter-6': 'FST00106',
        'Sanforiser-2': 'FSN00102',
        'Sanforiser-3': 'FSN00103',
        'Sanforiser-4': 'FSN00104',
        'Curing': 'FCU00101',
        'Airo-24': 'FAR00401',
        'Thermofix': 'FTF00201',
        'Relax-Drier': 'FRD00101'
    }

    month = date_.month

    all_total_machines = list(machine_mapping.keys())
    unique_po_numbers_rundata = db.query(models.RunData.machine, models.RunData.po_number
                                         ).filter(
        extract('month', models.RunData.date_) == month,
        models.RunData.machine.in_(all_total_machines),
        models.RunData.operation_name == 'HEATSETT',
        models.RunData.duration > 0.0028
    ).distinct().subquery()

    unique_data = db.query(unique_po_numbers_rundata).all()

    result_dict = []
    for machine, po_number in unique_data:
        stm = sqlalchemy.select(
            models.PoData.machine,
            models.PoData.po_number,
            models.PoData.article,
            models.PoData.finish_glm,
            func.cast(models.PoData.hmi_data, JSONB).label('hmi_data')
        ).filter(
            models.PoData.machine == machine,
            models.PoData.po_number == po_number
        ).subquery()

        quantity = db.query(stm).all()

        po_counts_subquery = db.query(
            models.RunData.machine,
            models.RunData.po_number,
            func.count(models.RunData.po_number).label('po_qty')
        ).join(
            models.PoData,
            (models.RunData.po_number == models.PoData.po_number) & (
                    models.RunData.machine == models.PoData.machine)
        ).filter(
            extract('month', models.RunData.date_) == month,
            models.RunData.machine == machine,
            models.RunData.po_number == po_number,
            models.RunData.operation_name == 'HEATSETT'
        ).group_by(
            models.RunData.machine,
            models.RunData.po_number,
            models.RunData.date_
        ).subquery()
        count = db.query(po_counts_subquery).all()

        date_subquery = db.query(
            models.RunData.date_,
        ).join(
            models.PoData,
            (models.RunData.po_number == models.PoData.po_number) & (
                    models.RunData.machine == models.PoData.machine)
        ).filter(
            extract('month', models.RunData.date_) == month,
            models.RunData.machine == machine,
            models.RunData.po_number == po_number,
            models.RunData.operation_name == 'HEATSETT'
        ).subquery()
        date_run = db.query(date_subquery).all()

        meter_subquery = db.query(
            models.RunData.machine,
            models.RunData.po_number,
            func.sum(models.RunData.meters).label('meter')
        ).join(
            models.PoData,
            (models.RunData.po_number == models.PoData.po_number) & (
                    models.RunData.machine == models.PoData.machine)
        ).filter(
            extract('month', models.RunData.date_) == month,
            models.RunData.machine == machine,
            models.RunData.po_number == po_number,
            models.RunData.operation_name == 'HEATSETT'
        ).group_by(
            models.RunData.machine,
            models.RunData.po_number,
            models.RunData.date_
        ).subquery()
        meter = db.query(meter_subquery).all()

        duration_subquery = db.query(
            models.RunData.machine,
            models.RunData.po_number,
            func.sum(models.RunData.duration).label('total_duration')
        ).join(
            models.PoData,
            (models.RunData.po_number == models.PoData.po_number) & (
                    models.RunData.machine == models.PoData.machine)
        ).filter(
            extract('month', models.RunData.date_) == month,
            models.RunData.machine == machine,
            models.RunData.po_number == po_number,
            models.RunData.operation_name == 'HEATSETT'
        ).group_by(
            models.RunData.machine,
            models.RunData.po_number
        ).subquery()
        total_duration = db.query(duration_subquery).all()

        speed_subquery = db.query(
            models.RunData.machine,
            models.RunData.po_number,
            (func.sum(models.RunData.meters) / (func.sum(models.RunData.duration) / 60)).label('speed')
        ).join(
            models.PoData,
            (models.RunData.po_number == models.PoData.po_number) & (
                    models.RunData.machine == models.PoData.machine)
        ).filter(
            extract('month', models.RunData.date_) == month,
            models.RunData.machine == machine,
            models.RunData.po_number == po_number,
            models.RunData.operation_name == 'HEATSETT'
        ).group_by(
            models.RunData.machine,
            models.RunData.po_number
        ).subquery()
        speed = db.query(speed_subquery).all()

        if quantity:
            data = {
                "machine": quantity[0][0],
                "po_number": quantity[0][1],
                "article": quantity[0][2],
                "finish_glm": quantity[0][3],
                "hmi_data": quantity[0][4],
                "po_qty": count[0][2] if count else 0,
                "meter": meter[0][2] if meter else 0,
                "date_run": date_run[0][0],
                "total_duration": total_duration[0][2] if total_duration else 0,
                "speed": speed[0][2] if speed else 0
            }
            result_dict.append(data)

    # Sort result_dict by date_run
    result_dict = sorted(result_dict, key=lambda x: x['date_run'])

    processed_data = []

    for row in result_dict:
        print(row)
        machine, po_number, article, finish_glm, hmi_data, po_qty, meter, date_run, total_duration, speed = row

        formatted_machine = machine_mapping.get(row['machine'], row['machine'])

        if article is None:
            k1, k2, k3, k4, k5 = [None] * 5
        else:
            article_parts = row['article'].split(',')
            k1 = article_parts[0] if len(article_parts) > 0 else None
            k2 = article_parts[1] if len(article_parts) > 1 else None
            k3 = article_parts[2] if len(article_parts) > 2 else None
            k4 = article_parts[3] if len(article_parts) > 3 else None
            k5 = article_parts[4] if len(article_parts) > 4 else None

        if article is None:
            finish = None
        else:
            finish = f'{k3}'

        if isinstance(row['hmi_data'], str):
            try:
                row['hmi_data'] = json.loads(hmi_data)
            except json.JSONDecodeError:
                row['hmi_data'] = {}
        if isinstance(row['hmi_data'], dict):
            tw_values = row['hmi_data'].get('tw_values', {})
            temperature = tw_values.get('temperature')
            width = tw_values.get('width')
        else:
            temperature = width = None

        row_dict = {
            'po_number': row['po_number'],
            'machine': formatted_machine,
            'date_': row['date_run'],
            'article': row['article'],
            'finish_glm': int(row['finish_glm']),
            'temperature': temperature,
            'width': width,
            'k1': k1,
            'k2': k2,
            'k3': k3,
            'k4': k4,
            'Finish': k3,
            'po_qty': row['po_qty'],
            'meter': int(row['meter']),
            'speed': round(row[speed], 2)
        }
        processed_data.append(row_dict)

    return processed_data


async def get_meter_production_by_shift(date_: date, machine: str, db: Session):
    # Query to sum the meters column grouped by the shift
    meter_sums = db.query(
        models.RunData.shift,
        func.sum(models.RunData.meters).label('meter_sum')
    ).filter(
        models.RunData.date_ == date_,
        models.RunData.machine == machine
    ).group_by(models.RunData.shift).all()

    # Initialize sums for each shift
    shift_sums = {"A": 0, "B": 0, "C": 0}

    # Update the shift sums with the query results
    for result in meter_sums:
        shift_sums[result.shift] = round(result.meter_sum, 2)  # Round to 2 decimal places

    # If no data found, raise an exception
    if all(value == 0 for value in shift_sums.values()):
        raise HTTPException(status_code=404, detail="Data not found")

    # Return the response with the date, machine, and shift sums
    return {
        "date": date_,
        "machine": machine,
        "A": shift_sums["A"],
        "B": shift_sums["B"],
        "C": shift_sums["C"]
    }


async def get_duration_per_machine(db: Session, date_: date):
    # Hardcoded list of machines
    machines = ["MONTI 1", "MONTI 2", "SINGEING-1", "SINGEING-2", "PTR", "PERBLE RANGE", "OLD MR", "NMR", "NPS",
                "Batcher-1", "Batcher-2", "XETMA-2", "XETMA-3", "LAFER-1", "LAFER-2", "LAFER-3", "LAFER-4",
                "LAFER 5-RASING",
                "LAFER 5-SHARING", "SOAPER-2"]
    stop_categories = ['Machanical Breakdown', 'Electrical Breakdown', 'Machine cleaning', 'Fabric changeover',
                       'Preventive Maintenance', 'No Power', 'No Steam', 'Man Power Shortage', 'No Program',
                       'Insect Problem', 'No Trolley']

    # Query the database to filter by date, machines, and stop_categories
    results = db.query(models.StopData).filter(
        models.StopData.date_ == date_,
        models.StopData.machine.in_(machines),
        models.StopData.stop_category.in_(stop_categories)  # Filtering for stop categories
    ).all()

    # Dictionary to store total duration for each machine and each stop category
    machine_durations = {machine: {category: 0 for category in stop_categories} for machine in machines}

    # Calculate the total duration from stop_time and start_time for each machine and stop category
    for result in results:
        if result.start_time and result.stop_time:
            duration_seconds = (result.stop_time - result.start_time).total_seconds()
            machine_durations[result.machine][result.stop_category] += duration_seconds / 3600  # Convert to hours

    # Round each duration to 2 decimal places
    rounded_durations = {
        machine: {category: round(duration, 2) for category, duration in categories.items()}
        for machine, categories in machine_durations.items()
    }

    return {"machine_durations": rounded_durations}


async def get_mtd_duration_machine_for_date_Range(db: Session, date_: date):
    # Hardcoded list of machines
    print("Start")
    machines = ["MONTI 1", "MONTI 2", "SINGEING-1", "SINGEING-2", "PTR", "PERBLE RANGE", "OLD MR", "NMR", "NPS",
                "Batcher-1", "Batcher-2", "XETMA-2", "XETMA-3", "LAFER-1", "LAFER-2", "LAFER-3", "LAFER-4",
                "LAFER 5-RASING",
                "LAFER 5-SHARING", "SOAPER-2"]
    stop_categories = ["MTD  MECH  BRKD.", "MTD  ELEC  BRKD."]
    print("stop category")
    # Calculate the start of the month
    start_date = date_.replace(day=1)

    # Query the database to filter by date range, machines, and stop_categories
    results = db.query(models.StopData).filter(
        models.StopData.date_.between(start_date, date_),
        models.StopData.machine.in_(machines),
        models.StopData.stop_category.in_(stop_categories)  # Filtering for stop categories
    ).all()

    # Dictionary to store total duration for each machine and each stop category
    machine_durations = {machine: {category: 0 for category in stop_categories} for machine in machines}
    print("machine suration")
    # Calculate the total duration from stop_time and start_time for each machine and stop category
    for result in results:
        if result.start_time and result.stop_time:
            duration_seconds = (result.stop_time - result.start_time).total_seconds()
            machine_durations[result.machine][result.stop_category] += duration_seconds / 3600  # Convert to hours
    print("result")
    # Round each duration to 2 decimal places
    rounded_durations = {
        machine: {category: round(duration, 2) for category, duration in categories.items()}
        for machine, categories in machine_durations.items()
    }
    print("rounded")
    return {"machine_durations": rounded_durations}


##......................................Email List.................................
async def create_email_list(email_list: schemas.EmailListCreate, db: Session):
    email_ids_str = ','.join([item.email_id_list for item in email_list.email_id_list])
    db_email_list = db.query(models.EmailList).filter(models.EmailList.section == email_list.section).first()
    if db_email_list:
        db_email_list.email_id_list = email_ids_str
    else:
        db_email_list = models.EmailList(
            section=email_list.section,
            email_id_list=email_ids_str
        )
        db.add(db_email_list)
    db.commit()
    db.refresh(db_email_list)
    return db_email_list


async def get_email_list_id(db: Session, id: int):
    db_email_list = db.query(models.EmailList).filter(models.EmailList.id == id).first()
    if not db_email_list:
        raise HTTPException(status_code=404, detail="Email list not found")
    return db_email_list


async def update_email_list(db: Session, email_data: schemas.EmailListUpdate, id: int):
    try:
        data_db = await get_email_list_id(db, id)
        if email_data.email_id_list:
            email_data.email_id_list = ','.join(email_data.email_id_list)
        for key, value in email_data.dict(exclude_unset=True).items():
            setattr(data_db, key, value)
        db.add(data_db)
        db.commit()
        db.refresh(data_db)
        return data_db
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{e}")


async def get_email_list_by_section(db: Session, section: str):
    db_email_list = db.query(models.EmailList).filter(models.EmailList.section == section).first()
    if not db_email_list:
        raise HTTPException(status_code=404, detail="Email list not found for the given section")
    return db_email_list


async def get_all_email_list(db: Session):
    db_email_list = db.query(models.EmailList.id, models.EmailList.email_id_list, models.EmailList.section).all()

    result = []
    for email_entry in db_email_list:
        email_list = email_entry.email_id_list.split(',')
        email_list = [email.strip() for email in email_list]
        result.append({
            "id": email_entry.id,
            "section": email_entry.section,
            "email_list": email_list
        })
    return result


async def get_id_list_by_section(db: Session, section: str):
    db_email_list = db.query(models.EmailList.email_id_list).filter(models.EmailList.section == section).first()
    if not db_email_list:
        raise HTTPException(status_code=404, detail="Email list not found for the given section")
    email_list = db_email_list.email_id_list.split(",")
    return email_list


async def get_run_per_day_(db: Session, start_date: date, end_date: date, machine: str):
    # Subquery to aggregate data from run_data
    stm1 = (
        sqlalchemy.select(
            sqlalchemy.func.max(models.RunData.date_).label('date_'),
            sqlalchemy.func.min(models.RunData.start_time).label('start_time'),
            sqlalchemy.func.max(models.RunData.stop_time).label('end_time'),
            models.RunData.po_number.label('po_number'),
            sqlalchemy.func.max(models.RunData.shift).label('shift'),
            sqlalchemy.func.sum(models.RunData.meters).label('production'),
            (sqlalchemy.func.sum(models.RunData.meters) / sqlalchemy.func.sum(models.RunData.duration) * 60).label(
                'speed')
        )
        .filter(
            models.RunData.date_.between(start_date, end_date),
            models.RunData.meters > 1,
            models.RunData.run_category != "Lead cloth",
            models.RunData.machine == machine
        )
        .group_by(models.RunData.po_number)
        .subquery()
    )

    # Subquery to get unique po_number with one article (e.g., latest or max)
    po_data_subq = (
        sqlalchemy.select(
            models.PoData.po_number,
            sqlalchemy.func.max(models.PoData.article).label("article")  # pick one article per po_number
        )
        .group_by(models.PoData.po_number)
        .subquery()
    )

    # Join aggregated RunData with PoData subquery
    joined_query = (
        db.query(
            stm1.c.date_,
            stm1.c.start_time,
            stm1.c.end_time,
            stm1.c.po_number,
            stm1.c.shift,
            stm1.c.production,
            stm1.c.speed,
            po_data_subq.c.article
        )
        .outerjoin(po_data_subq, stm1.c.po_number == po_data_subq.c.po_number)
        .order_by(stm1.c.date_.asc(), stm1.c.shift.asc(), stm1.c.start_time.asc())
    )

    run_po_data = joined_query.all()

    # Create DataFrame
    run_po_data_df = pd.DataFrame(run_po_data, columns=[
        'date_', 'start_time', 'end_time', 'po_number', 'shift', 'production', 'speed', 'article'
    ])

    # Optional conversions
    try:
        run_po_data_df['production'] = run_po_data_df['production'].astype(int)
    except:
        pass
    try:
        run_po_data_df['speed'] = run_po_data_df['speed'].astype(int)
    except:
        pass

    return run_po_data_df.sort_values(['date_', 'shift', 'start_time']).to_dict("records")
