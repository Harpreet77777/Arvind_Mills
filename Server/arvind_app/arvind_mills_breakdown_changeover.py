import logging
import pytz
from datetime import datetime, time, timedelta
import psycopg2
import time as tp
import schedule

# =====================================================
# LOGGING CONFIGURATION
# =====================================================

logging.basicConfig(
    filename='breakdown_changeover.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

log = logging.getLogger()

# =====================================================
# DATABASE CONFIGURATION
# =====================================================

DB_NAME = "arvind_mills_db"
DB_USER = "postgres"
DB_PASSWORD = "Cybershot#903"
DB_HOST = "localhost"
DB_PORT = 5432

# =====================================================
# SHIFT TIMINGS
# =====================================================

shift_a_start = time(8, 0, 0)
shift_a_end = time(12, 17, 0)
shift_b_start = time(12, 17, 0)
# shift_b_start = time(16, 0, 0)
shift_b_end = time(0, 0, 0)

shift_c_start = time(0, 0, 0)
shift_c_end = time(8, 0, 0)


# =====================================================
# GET CURRENT SHIFT
# =====================================================

def getShift():
    now = datetime.now(
        pytz.timezone('Asia/Kolkata')
    ).time()

    # Shift A → 08:00 to 16:00
    if shift_a_start <= now < shift_a_end:
        return 'A'

    # Shift B → 16:00 to 00:00
    elif shift_b_start <= now <= time(23, 59, 59):
        return 'B'

    # Shift C → 00:00 to 08:00
    else:
        return 'C'


# =====================================================
# GET SHIFT END TIME
# =====================================================

def get_shift_end_datetime(
        record_shift,
        record_date,
        timezone
):
    if record_shift == 'A':

        naive_datetime = datetime.combine(
            record_date,
            shift_a_end
        )

    elif record_shift == 'B':

        naive_datetime = datetime.combine(
            record_date + timedelta(days=1),
            shift_b_end
        )

    else:

        naive_datetime = datetime.combine(
            record_date,
            shift_c_end
        )

    localized_datetime = timezone.localize(
        naive_datetime
    )

    stop_time = datetime.utcnow() + timedelta(hours=5, minutes=30)

    return stop_time


# =====================================================
# STOP BREAKDOWN
# =====================================================

def stop_breakdown_data(
        cur,
        conn,
        record_id,
        stop_time,
        duration
):
    cur.execute("""
        UPDATE breakdown_data
        SET
            stop_time = %s,
            duration = %s
        WHERE id = %s;
    """,
                (
                    stop_time,
                    duration,
                    record_id
                ))

    conn.commit()

    log.info(
        f"Stopped breakdown id "
        f"{record_id}"
    )


# =====================================================
# START BREAKDOWN
# =====================================================

def start_breakdown_data(
        cur,
        conn,
        date_,
        shift,
        machine_name,
        line,
        start_time,
        breakdown_po_uuid,
        category,
        reason
):
    cur.execute("""
        INSERT INTO breakdown_data
        (
            date_,
            shift,
            machine_name,
            line,
            start_time,
            breakdown_po_uuid,
            category,
            reason
        )
        VALUES
        (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        );
    """,
                (
                    date_,
                    shift,
                    machine_name,
                    line,
                    start_time,
                    breakdown_po_uuid,
                    category,
                    reason
                ))

    conn.commit()

    log.info(
        f"Started new breakdown for "
        f"{machine_name}"
    )


# =====================================================
# MAIN FUNCTION
# =====================================================

def breakdown_changeover_shift_wise():
    conn = psycopg2.connect(
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

    cur = conn.cursor()

    today = datetime.now(
        pytz.timezone('Asia/Kolkata')
    ).date()

    present_shift = getShift()

    kolkata_timezone = pytz.timezone(
        'Asia/Kolkata'
    )

    # =====================================================
    # FETCH ACTIVE BREAKDOWNS
    # =====================================================

    cur.execute("""
        SELECT
            id,
            date_,
            shift,
            machine_name,
            line,
            start_time,
            breakdown_po_uuid,
            category,
            reason
        FROM breakdown_data
        WHERE stop_time IS NULL;
    """)

    data = cur.fetchall()

    # =====================================================
    # PROCESS RECORDS
    # =====================================================

    for record in data:

        log.info(f"Processing record: {record}")

        (
            record_id,
            record_date,
            record_shift,
            machine_name,
            line,
            start_time,
            breakdown_po_uuid,
            category,
            reason
        ) = record

        # =====================================================
        # SHIFT CHANGE DETECTED
        # =====================================================

        if (
                record_shift != present_shift
                or record_date != today
        ):
            stop_time = get_shift_end_datetime(
                record_shift,
                record_date,
                kolkata_timezone
            )

            # Convert stop_time to naive datetime

            # stop_time = stop_time.replace(tzinfo=None)
            stop_time = datetime.utcnow() + timedelta(hours=5, minutes=30)

            duration = (stop_time - start_time).total_seconds()
            print(
                f"Stop time: {stop_time}"
            )

            print(
                f"Duration: {duration}"
            )

            # =====================================================
            # STOP OLD BREAKDOWN
            # =====================================================

            stop_breakdown_data(
                cur=cur,
                conn=conn,
                record_id=record_id,
                stop_time=stop_time,
                duration=duration
            )

            # =====================================================
            # START NEW BREAKDOWN
            # =====================================================

            start_breakdown_data(
                cur=cur,
                conn=conn,
                date_=today,
                shift=present_shift,
                machine_name=machine_name,
                line=line,
                start_time=stop_time,
                breakdown_po_uuid=breakdown_po_uuid,
                category=category,
                reason=reason
            )

    cur.close()

    conn.close()


# =====================================================
# SCHEDULER
# =====================================================

schedule.every(1).minutes.do(
    breakdown_changeover_shift_wise
)

# =====================================================
# RUN FOREVER
# =====================================================

while True:
    schedule.run_pending()

    tp.sleep(1)
