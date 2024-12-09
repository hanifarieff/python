from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine as ce
import pandas as pd
import numpy as np
import time
import datetime as dt
date = dt.datetime.today()
import sys

sys.stdout = open("C:/TestPython/DWH_MySQL/logs/LogBIOS01DMDataSDM.txt","w")

t0 = time.time()
dwh_mysql = ce("mysql://hanif-ppi:hanif2022@172.16.5.33/dwhrscm_talend")
try :
    conn_dwh_mysql = dwh_mysql.connect()
    #con_sqlserver = sql_server.connect()
    print('successfully connect to all database')
    
except SQLAlchemyError as e:
    error=str(e.__dict__['orig'])
    print(error)


def inject_data(*list_table):
    for index,val in enumerate(list_table, start=1):
        try:
            query = (f"SELECT TglTransaksi,Total,EmployeeGroup,IsSent FROM {val} ORDER BY TglTransaksi")
            source = pd.read_sql_query(query, conn_dwh_mysql)
            source.insert(1,'StandardRefID',index)
            print(source)
            source.to_sql('BIOS01DMDataSDM', con=conn_dwh_mysql, if_exists='append',index=False)
            msg = f'success insert {val}\n'
            print(msg)
        except Exception as e:
            print(e)

inject_data('01DMAhliTeknologiLaboratoriumMedik_V3',
'02DMPharmacist_V3','03DMBidan_V3','04DMNutrisionis_V3',
'05DMDokterUmum_V3','06DMDokterGigi_V3','07DMDokterSpesialis_V3','08DMFisioterapis_V3','09DMPerawat_V3','10DMRadiografer_V3',
'11DMTenagaProfesionalSelainyangdisebutkandiatas_V3','12DMTenagaNonMedis_V3','87DMSanitarian')


t1 = time.time()
total=t1-t0
print(total)

text=f'scheduler tanggal : {date}'
print(text)

conn_dwh_mysql.close()
sys.stdout.close()