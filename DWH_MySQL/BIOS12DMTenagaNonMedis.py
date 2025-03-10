from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine as ce
import pandas as pd
import numpy as np
import time
import datetime as dt
date = dt.datetime.today()
import sys

sys.stdout = open("C:/TestPython/DWH_MySQL/logs/LogBIOS12DMTenagaNonMedis.txt","w")
t0 = time.time()

dwh_mysql = ce("mysql://hanif-ppi:hanif2022@172.16.5.33/dwhrscm_talend")
try :
    conn_dwh_mysql = dwh_mysql.connect()
    #con_sqlserver = sql_server.connect()
    print('successfully connect to all database')
    
except SQLAlchemyError as e:
    error=str(e.__dict__['orig'])
    print(error)

source = pd.read_sql_query(""" SELECT TglTransaksi,Total,EmployeeGroup,IsSent FROM 12DMTenagaNonMedis_V3""", conn_dwh_mysql)
try:
    source.to_sql('BIOS12DMTenagaNonMedis', con=conn_dwh_mysql, if_exists='append',index=False)
    print('success insert')
    print(source)
except Exception as e:
    print(Exception())

t1 = time.time()
total=t1-t0
print(total)

text=f'scheduler tanggal : {date}'
print(text)

conn_dwh_mysql.close()
sys.stdout.close()