import logging
import pymssql

log = logging.getLogger()


class EmployeeDBHelper():
    def __init__(self):
        self.server = '172.28.3.193'
        self.user = 'admin'
        self.password = 'P$bedi#9091'
        self.db = 'IIOT'

        # self.c.execute('''CREATE TABLE GrindingData(Sno INTEGER IDENTITY(1,1) PRIMARY KEY, DateTime_ DATETIME NOT NULL,
        #                 MachineNumber TEXT NOT NULL,PartNumber TEXT NOT NULL, ModelNumber TEXT DEFAULT ('TS02-S1'))''')

    def check_employee(self, employee_id: str, password: str):
        try:
            self.conn = pymssql.connect(server=self.server, user=self.user, password=self.password,
                                        database=self.db)
            self.c = self.conn.cursor()
            print(self.conn)

            employee_id = employee_id.strip()
            if employee_id.strip() == "":
                return False
            self.c.execute(f'''SELECT EmployeeName FROM EmployeeMaster WHERE EmployeeCode LIKE '{employee_id}%';''')
            check_ms = self.c.fetchone()

            self.conn.close()

            print(f'CHECK MS DATA: {check_ms}')
            if check_ms is None:
                return False
            else:
                pwd = check_ms[0].replace(" ", "").lower()[0:4]
                print(pwd)
                if pwd == password.lower():
                    return True
                else:
                    return False
        except Exception as e:
            log.error('ERROR: ' + str(e) + ' Could not add SCAN DATA to MS SQL SERVER.')
            return False

    def get_employees(self):
        try:
            self.conn = pymssql.connect(server=self.server, user=self.user, password=self.password,
                                        database=self.db)
            self.c = self.conn.cursor()
            print(self.conn)
            self.c.execute(f'''SELECT TOP 10 * FROM  EmployeeMaster ;''')
            check_ms = self.c.fetchall()
            print(type(check_ms), len(check_ms))

            log.info(f'CHECK MS DATA: {check_ms}')
            return True
        except Exception as e:
            log.error('ERROR: ' + str(e) + ' Cannot fetch EMPLOYEE data.')
            return False


class StagingDBHelper():
    def __init__(self):
        self.server = '172.27.1.67'
        self.user = 'sa'
        self.password = 'Gt@vc@345'
        self.db = 'NOW_STAGING'

    def addProductionData(self, ProductionData):
        try:
            self.conn = pymssql.connect(server=self.server, user=self.user, password=self.password,
                                        database=self.db)
            self.c = self.conn.cursor()
            print(self.conn)
            print(ProductionData.production)
            params_data = [tuple(items) for items in ProductionData.production]
            self.c.executemany(f'''INSERT INTO [dbo].[Production]
                               ([PROGRESSPARTIALENDDATE]
                               ,[PROGRESSPARTIALENDTIME]
                               ,[PRODUCTIONORDERCODE]
                               ,[MACHINECODE]
                               ,[OPERATIONCODE]
                               ,[RUNCATEGORY]
                               ,[CALSHIFTDAILYINFORMATION]
                               ,[PRIMARYQTYMT]
                               ,[OPERATORCODE]
                               ,[MachineRunningSpeed]
                               ,[mACHINErUNNINGtIME]
                               ,[Power]
                               ,[Steam]
                               ,[Heat]
                               ,[Water]
                               ,[air]
                               ,[GPL])
                               VALUES(%s,%s,%s,%s,
                                       %s,%s,%s,%s,
                                       %s,%s,%s,%s,
                                       %s,%s,%s,%s,%s)''', params_data)
            self.conn.commit()
            log.info('Successful: Production added to the NOW Staging SQL SERVER')
            print('Successful: Production added to the NOW Staging SQL SERVER')
            self.conn.close()
            return True
        except Exception as e:
            log.error('ERROR: ' + str(e) + ' Could not add Production DATA to NOW Staging SQL SERVER.')
            return False

    def addStoppageData(self, StoppageData):
        try:
            self.conn = pymssql.connect(server=self.server, user=self.user, password=self.password,
                                        database=self.db)
            self.c = self.conn.cursor()
            print(self.conn)
            print(StoppageData.stoppage)
            params_data = [tuple(items) for items in StoppageData.stoppage]
            self.c.executemany(f'''INSERT INTO [dbo].[Stoppage]
                               ([PROGRESSPARTIALENDDATE]
                              ,[PROGRESSPARTIALENDTIME]
                              ,[PRODUCTIONORDERCODE]
                              ,[MACHINECODE]
                              ,[OPERATIONCODE]
                              ,[CALSHIFTDAILYINFORMATION]
                              ,[StoppageCode]
                              ,[PROCESSRECORDEDMACHINETIME]
                              ,[OPERATORCODE]
                              ,[Power]
                              ,[Steam]
                              ,[Heat]
                              ,[Water]
                              ,[air]
                              ,[GPL])
                               VALUES(%s,%s,%s,%s,
                                       %s,%s,%s,%s,
                                       %s,%s,%s,%s,
                                       %s,%s,%s)''', params_data)
            self.conn.commit()
            log.info('Successful: Stoppage added to the NOW Staging SQL SERVER')
            print('Successful: Stoppage added to the NOW Staging SQL SERVER')
            self.conn.close()
            return True
        except Exception as e:
            log.error('ERROR: ' + str(e) + ' Could not add Stoppage DATA to NOW Staging SQL SERVER.')
            return False


if __name__ == '__main__':
    ms_db = EmployeeDBHelper()
    ms_db.get_employees()
