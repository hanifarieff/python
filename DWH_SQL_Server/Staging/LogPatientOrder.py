import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pandas as pd
import pyodbc
import datetime as dt
date = dt.datetime.today()
import time

# bikin log ke file
sys.stdout = open("C:/TestPython/DWH_SQL_Server/Staging/logs/LogRowPatientOrder.txt","w")
text=f'scheduler tanggal : {date}'
print(text)
t0 = time.time()

# bikin koneksi ke db
conn_ehr_live = db_connection.create_connection(db_connection.ehr_live)
conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)

# source dari ehr live
query = f""" SELECT 
            NOW() as extract_date,
            DATE(created_dttm) as created_date,  
            COUNT(*) as total_row
            FROM xocp_ehr_patient_order
            where created_dttm >= '2024-12-02 00:00:00' and created_dttm <= '2024-12-02 23:59:59'
            """
source = pd.read_sql_query(query, conn_ehr_live)
print(source)

try:
    source.to_sql('LogPatientOrder',schema='staging_rscm', con = conn_staging_sqlserver, if_exists='append',index=False)
    print('success insert')
except Exception as e:
    print(e)

t1 = time.time()
total=t1-t0
print(total)

text= f'scheduler tanggal : {date}'
print(text)
sys.stdout.close()

db_connection.close_connection(conn_ehr_live)
db_connection.close_connection(conn_staging_sqlserver)

sys.stdout.close()