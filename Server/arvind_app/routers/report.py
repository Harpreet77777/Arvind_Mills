import os
import sys
from openpyxl import load_workbook
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from fastapi.responses import FileResponse
from fastapi import Depends, FastAPI, HTTPException, UploadFile
from openpyxl.utils import get_column_letter
from sqlalchemy.orm import Session
from sqlalchemy import cast, Time, or_, and_
from datetime import date
from .. import crud, models, schemas
from ..database import SessionLocal, engine
import pytz
from fastapi import Depends, APIRouter
import logging

log = logging.getLogger("uvicorn")
log.setLevel(logging.INFO)

IST = pytz.timezone('Asia/Kolkata')

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


router = APIRouter(tags=["Report"])


@router.get("/router/get_report/{from_date}/{to_date}")
async def report(from_date: date, to_date: date, db: Session = Depends(get_db)):
    po_data = db.query(models.PoData).filter(models.PoData.date_.between(from_date, to_date),
                                             models.PoData.stop_time != None).order_by(models.PoData.id.asc()).all()

    report_data = []

    for item in po_data:
        hourly_data = db.query(models.HourlyData).filter(models.HourlyData.po_uuid == item.po_uuid).order_by(
            models.HourlyData.id.desc()).all()

        last_length = None
        last_speed = None

        # Only process hourly_data if it exists
        if hourly_data:
            for data in hourly_data:
                if data.key == "Length" and last_length is None:
                    last_length = data.key_stop
                elif data.key == "Speed" and last_speed is None:
                    last_speed = data.key_stop

        report_data.append({
            **item.__dict__,
            "last_length": last_length,
            "last_speed": last_speed
        })
    return report_data


@router.get("/report/generate_history_po_data_report/{from_date}/{to_date}")
async def generate_history_po_data_report(from_date: date, to_date: date, db: Session = Depends(get_db)):
    try:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        template_path = os.path.join(base_dir, "Reports", "Template", "history_po_data_template.xlsx")
        wb = load_workbook(template_path)
        ws = wb.active
        data = await report(from_date, to_date, db)

        for item in data:
            item.pop("id", None)

        headers = ["date_", "machine_name", "line", "shift", "po_number", "section", "category",
                   "machine_speed", "machine_speed_unit", "operation", "start_time", "stop_time", "duration",
                   "operator_name", "target_length", "target_unit", "last_length", "last_speed",
                   "is_partial_gr", "is_complete"]

        header_fill = PatternFill(start_color="808080", end_color="808080", fill_type="solid")
        header_font = Font(bold=True, color="FFFFFF")
        center_alignment = Alignment(horizontal="center", vertical="center")

        thin_border = Border(left=Side(style='thin'), right=Side(style='thin'),
                             top=Side(style='thin'), bottom=Side(style='thin'))

        for col_num, header in enumerate(headers, start=1):
            cell = ws.cell(row=2, column=col_num)
            cell.value = header.upper()
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = center_alignment
            cell.border = thin_border

        for row_num, item in enumerate(data, start=3):
            for col_num, header in enumerate(headers, start=1):
                cell = ws.cell(row=row_num, column=col_num)
                cell.value = item.get(header)
                cell.alignment = center_alignment
                cell.border = thin_border

        for col_num in range(1, len(headers) + 1):
            column_letter = get_column_letter(col_num)
            max_length = 0
            for row in ws.iter_rows(min_row=2, max_row=ws.max_row, min_col=col_num, max_col=col_num):
                cell = row[0]
                try:
                    if cell.value:
                        max_length = max(max_length, len(str(cell.value)))
                except:
                    pass
            ws.column_dimensions[column_letter].width = max_length + 5

        file_path = os.path.join(base_dir, "Reports", "history_po_data_report.xlsx")
        wb.save(file_path)
        return FileResponse(path=file_path,
                            filename=f"PO Data Report_{from_date}_to_{to_date}.xlsx",
                            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    except Exception as e:
        return {"error": str(e)}
