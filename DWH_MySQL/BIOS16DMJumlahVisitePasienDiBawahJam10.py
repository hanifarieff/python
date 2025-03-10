from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine as ce
import pandas as pd
import numpy as np
import time
import datetime as dt
date = dt.datetime.today()
import sys

sys.stdout = open("C:/TestPython/DWH_MySQL/logs/LogBIOS16DMJumlahVisitePasienDiBawahJam10.txt","w")

t0 = time.time()

dwh_mysql = ce("mysql://hanif-ppi:hanif2022@172.16.5.33/dwhrscm_talend")
try :
    conn_dwh_mysql = dwh_mysql.connect()
    #con_sqlserver = sql_server.connect()
    print('successfully connect to all database')
    
except SQLAlchemyError as e:
    error=str(e.__dict__['orig'])
    print(error)

source = pd.read_sql_query(""" SELECT TglTransaksi, Jumlah FROM 16DMJumlahVisitePasienDiBawahJam10
                                
                                WHERE TglTransaksi >= DATE_SUB(DATE(now()), interval 10 DAY)
                                AND TglTransaksi <= DATE_SUB(DATE(now()),interval 1 day)
                                ORDER BY TglTransaksi
                            """, conn_dwh_mysql)

# ambil primary key dari source, pake unique biar tidak duplicate
source['TglTransaksi'] = pd.to_datetime(source.TglTransaksi, format='%Y-%m-%d')
source['TglTransaksi'] = source['TglTransaksi'].dt.strftime('%Y-%m-%d')
tgltransaksi=tuple(source['TglTransaksi'].unique())
print(tgltransaksi)
print(source)

query = f'SELECT TglTransaksi,Jumlah FROM BIOS16DMJumlahVisitePasienDiBawahJam10 WHERE TglTransaksi IN {tgltransaksi} ORDER BY TglTransaksi'
target = pd.read_sql(query,conn_dwh_mysql)
target['TglTransaksi'] = pd.to_datetime(target.TglTransaksi,format='%Y-%m-%d')
target['TglTransaksi'] = target['TglTransaksi'].dt.strftime('%Y-%m-%d')
print(target)

changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

modified = changes[changes[['TglTransaksi']].apply(tuple,1).isin(target[['TglTransaksi']].apply(tuple,1))]
total_row_upd = len(modified)
text_upd = f'total row update : {total_row_upd}'
print(text_upd)
print(modified)

# ambil data yang new dari changes
inserted = changes[~changes[['TglTransaksi']].apply(tuple,1).isin(target[['TglTransaksi']].apply(tuple,1))]
total_row_ins = len(inserted)
text_ins = f'total row inserted : {total_row_ins}'
print(text_ins)
print(inserted)

if modified.empty:
    inserted['IsSent'] = 0
    inserted.to_sql('BIOS16DMJumlahVisitePasienDiBawahJam10', con=conn_dwh_mysql, if_exists='append',index=False)
    print('success insert all data without update')
else:
    # buat fungsi untuk update data ke tabel target 
    def updated_to_sql(df, table_name,key_1):
        list_col = []
        table=table_name
        pk_1 = key_1
        temp_table =f'{table}_temporary_table'
        for col in df.columns:
            if col == pk_1:
                continue
            list_col.append(f'r.{col}=t.{col}')
        df.to_sql(temp_table,con=conn_dwh_mysql, if_exists='replace',index=False)
        update_stmt_1 = f'UPDATE {table} r '
        update_stmt_2 = f'INNER JOIN (SELECT * FROM {temp_table}) as t ON r.{pk_1} = t.{pk_1} '
        update_stmt_3 = f'SET '
        update_stmt_4 = ", ".join(list_col)
        update_stmt_5 = f' WHERE r.{pk_1} = t.{pk_1} '
        update_stmt_6 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_4 + update_stmt_5 + ";"
        delete_stmt_1 = f'DROP TABLE {temp_table}'
        print(update_stmt_6)
        conn_dwh_mysql.execute(update_stmt_6)
        conn_dwh_mysql.execute(delete_stmt_1)
    
    try:
        #update data
        updated_to_sql(modified, 'BIOS16DMJumlahVisitePasienDiBawahJam10', 'TglTransaksi')

        #insert data baru
        inserted.to_sql('BIOS16DMJumlahVisitePasienDiBawahJam10', con=conn_dwh_mysql, if_exists='append',index=False)
        print('success update and insert all data')
    except Exception as e:
        print(e)

#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)

text=f'scheduler tanggal : {date}'
print(text)