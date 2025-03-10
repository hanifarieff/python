import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pandas as pd
import numpy as np
import time
import datetime as dt
date = dt.datetime.today()

# bikin log ke file
sys.stdout = open("C:/TestPython/DWH_SQL_Server/BIOS/logs/Log01DMDataSDM.txt","w")
t0 = time.time()

# bikin koneksi ke db
conn_dwh_sqlserver = db_connection.create_connection(db_connection.dwh_sqlserver)
conn_bios_sqlserver = db_connection.create_connection(db_connection.bios_sqlserver)

# bikin fungsi buat insert data secara looping
def inject_data(*list_table):
    for index,val in enumerate(list_table, start=1):
        try:
            query = (f"SELECT TglTransaksi,Total,EmployeeGroup,IsSent FROM dwhrscm_talend.[{val}] ORDER BY TglTransaksi")
            source = pd.read_sql_query(query, conn_dwh_sqlserver)
            source.insert(1,'StandardRefID',index)
            print(source)
            source.to_sql('BIOS01DMDataSDM', schema='dwhrscm_talend',con=conn_bios_sqlserver, if_exists='append',index=False)
            msg = f'success insert {val}\n'
            print(msg)
        except Exception as e:
            print(e)

inject_data('01DMAhliTeknologiLaboratoriumMedik',
'02DMPharmacist','03DMBidan','04DMNutrisionis',
'05DMDokterUmum','06DMDokterGigi','07DMDokterSpesialis','08DMFisioterapis','09DMPerawat','10DMRadiografer',
'11DMTenagaProfesionalSelainyangdisebutkandiatas','26DMTenagaNonMedis','37DMSanitarian')

t1 = time.time()
total=t1-t0
print(total)

text=f'scheduler tanggal : {date}'
print(text)

db_connection.close_connection(conn_dwh_sqlserver)
db_connection.close_connection(conn_bios_sqlserver)
sys.stdout.close()