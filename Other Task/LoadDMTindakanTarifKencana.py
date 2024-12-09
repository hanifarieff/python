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

sys.stdout = open("C:/TestPython/Other Task/logs/LogDMTindakanTarifKencana.txt","w")
t0 = time.time()

conn_his = db_connection.create_connection(db_connection.replika_his)
conn_ehr = db_connection.create_connection(db_connection.replika_ehr)
conn_ehr_live = db_connection.create_connection(db_connection.ehr_live)

# tarik database dari replika
source = pd.read_sql_query(""" SELECT a.order_id, a.order_no as role_no
                                FROM xocp_dm_tindakankencana a
                                where a.verified_dttm >= '2023-09-01 00:00:00' AND a.verified_dttm <= '2023-09-05 23:59:59'
                                """, conn_ehr)

print(source)
orderid=tuple(source['order_id'].unique())

query_get_proc = f"SELECT order_id, obj_id as proc_id FROM xocp_his_patient_order where order_id IN {orderid}"
proc = pd.read_sql_query(query_get_proc,conn_his)
print(proc.dtypes)

query_get_tarif = f"SELECT order_id, tariff as proc_tarif FROM xocp_his_billing_item where order_id IN {orderid}"
tarif = pd.read_sql_query(query_get_tarif,conn_his)
tarif['order_id']=tarif['order_id'].astype('int64')
print(tarif.dtypes)

query_get_role = f"SELECT order_id,role_no, clin_priv_id as role_id FROM xocp_his_patient_order_role where order_id IN {orderid}"
role = pd.read_sql_query(query_get_role,conn_his)
print(role.dtypes)

source= source.merge(proc,how='inner',on='order_id').merge(tarif,how='inner',on='order_id').merge(role,how='inner',on=['order_id','role_no'])
new_order_columns = ['order_id','proc_id','role_id','proc_tarif']

source=source.reindex(columns=new_order_columns)

order_id = source['order_id']
source=source.drop_duplicates()

# source['role_id'].fillna('0',inplace=True)
# convert semua kolom menyesuaikan tipe data yang ada di target
source['order_id'] = source['order_id'].astype('str')
source['proc_id'] = source['proc_id'].astype('str')
source['role_id'] = source['role_id'].astype('int64')
source['role_id'] = source['role_id'].astype('str')

# hapus data yg proc_tarifnya duplikat dan convert kolom proc_tarif ke integer
source.dropna(subset=['proc_tarif'], inplace=True)
source['proc_tarif'] = source['proc_tarif'].apply(lambda x: int(round(float(x))))
source['proc_tarif'] = source['proc_tarif'].astype('int64')
print('ini source')
print(source.dtypes)
print(source)

if source.empty:
    print('tidak ada data dari source')
else:
    orderid=tuple(source['order_id'].unique())
    procid =tuple(source['proc_id'].unique())
    roleid = tuple(source['role_id'].unique())

    query = f"SELECT order_id, proc_id, role_id, proc_tarif from xocp_dm_tindakantarif where order_id in {orderid} and proc_id in {procid} and role_id in {roleid}"
    target = pd.read_sql_query(query,conn_ehr_live)
    target['proc_tarif'] = pd.to_numeric(target['proc_tarif'], errors='coerce')
    target.dropna(subset=['proc_tarif'], inplace=True)
    target['proc_tarif'] = target['proc_tarif'].apply(lambda x: int(round(float(x))))
    target['proc_tarif'] = target['proc_tarif'].astype('int64')
    target['role_id'] = target['role_id'].astype('int64')
    target['role_id'] = target['role_id'].astype('str')
    print(target.dtypes)
    print(target)

    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

     # ambil data yang update dari changes
    modified = changes[changes[['order_id','proc_id','role_id']].apply(tuple,1).isin(target[['order_id','proc_id','role_id']].apply(tuple,1))]
    total_row_upd = len(modified)
    text_upd = f'total row update : {total_row_upd}'
    print(text_upd)
    print(modified)

    # ambil data yang new dari changes
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
db_connection.close_connection(conn_his)
db_connection.close_connection(conn_ehr_live)