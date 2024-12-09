import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pyodbc
import pandas as pd
import numpy as np
import time
import datetime as dt
date = dt.datetime.today()
import sys

sys.stdout = open("C:/TestPython/Other Task/logs/LogDMTindakanTarifEHR.txt","w")
t0 = time.time()

conn_ehr = db_connection.create_connection(db_connection.replika_ehr)
conn_ehr_live = db_connection.create_connection(db_connection.ehr_live)

# tarik data dari database EHR Replika
source = pd.read_sql_query(""" SELECT 
                                    a.order_id, 
                                    a.proc_id, 
                                    c.default_obj_id as role_id, 
                                    CASE
                                        WHEN b.tariff = '0' THEN d.tariff 
                                        ELSE b.tariff
                                    END as proc_tarif
                                FROM xocp_dm_tindakan a
                                LEFT JOIN xocp_ehr_patient_order b on a.order_id=b.order_id
                                INNER JOIN xocp_ehr_patient_order_role c on c.order_id = a.order_id and a.role_no = c.role_no
                                INNER JOIN xocp_ehr_payplan_obj d on a.proc_id = d.obj_id and d.payplan_id = 71
                                where a.verified_dttm >= '2023-09-25 00:00:00' AND a.verified_dttm <= '2023-09-26 23:59:59'
                                AND a.order_id LIKE '00%%'
                                -- AND a.order_id = '00200001046667'
                                GROUP BY a.order_id, a.proc_id,c.default_obj_id, b.tariff, d.tariff
                                """, conn_ehr)

print(source.dtypes)

# source['role_id'].fillna('-',inplace=True)
source['proc_tarif'] = pd.to_numeric(source['proc_tarif'], errors='coerce')

# hapus yang proc_tarifnya null lalu convert kolom proc_tarif jadi integer
source.dropna(subset=['proc_tarif'], inplace=True)
source['proc_tarif'] = source['proc_tarif'].apply(lambda x: int(round(float(x))))
source['proc_tarif'] = source['proc_tarif'].astype('Int64')

print(source)

if source.empty:
    print('tidak ada data dari source')
else:
    # jika data dari source cuma 1 row
    if len(source) == 1: 
        orderid=source['order_id'].values[0]
        procid =source['proc_id'].values[0]
        roleid =source['role_id'].values[0]

        query = f"SELECT order_id, proc_id, role_id, proc_tarif from xocp_dm_tindakantarif where order_id in ({orderid} )and proc_id in ({procid}) and role_id in ({roleid})"
        target = pd.read_sql_query(query,conn_ehr_live)
    #jika data dari source lebih dari 1 row
    else :
        orderid=tuple(source['order_id'])
        procid =tuple(source['proc_id'])
        roleid = tuple(source['role_id'])

        query = f"SELECT order_id, proc_id, role_id, proc_tarif from xocp_dm_tindakantarif where order_id in {orderid} and proc_id in {procid} and role_id in {roleid}"
        target = pd.read_sql_query(query,conn_ehr_live)
    
    print(target)
    print(target.dtypes)

    # deteksi perubahan antara source dan target
    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

    # ambil data yang berubah dari changes
    modified = changes[changes[['order_id','proc_id','role_id']].apply(tuple,1).isin(target[['order_id','proc_id','role_id']].apply(tuple,1))]
    total_row_upd = len(modified)
    text_upd = f'total row update : {total_row_upd}'
    print(text_upd)
    print(modified)

    # ambil data yang baru dari changes
    inserted = changes[~changes[['order_id','proc_id','role_id']].apply(tuple,1).isin(target[['order_id','proc_id','role_id']].apply(tuple,1))]
    total_row_ins = len(inserted)
    text_ins = f'total row inserted : {total_row_ins}'
    print(text_ins)
    print(inserted)

    if modified.empty :
        if inserted.empty :
            print('tidak ada data yang baru atau update')
        else:
            inserted.to_sql('xocp_dm_tindakantarif', con=conn_ehr_live, if_exists ='append',index=False)
            print('success inserted all data')
    else :
        if inserted.empty :
            print('tidak ada data yang baru yang bisa di insert')
        else:
            try:
                inserted.to_sql('xocp_dm_tindakantarif', con=conn_ehr_live, if_exists ='append',index=False)
                print('success inserted all data')
            except Exception as e:
                print(e)


#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)

text=f'scheduler tanggal : {date}'
print(text)

db_connection.close_connection(conn_ehr)
db_connection.close_connection(conn_ehr_live)