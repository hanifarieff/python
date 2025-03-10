from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine as ce
import pandas as pd
import pyodbc
import time
import datetime as dt
date = dt.datetime.today()
import sys

sys.stdout = open("C:/TestPython/LogFactDrugPrescription.txt","w")
t0 = time.time()
mysql = ce("mysql://hanif-ppi:hanif2022@172.16.19.11/ehr")
dwh = ce("mysql://hanif-ppi:hanif2022@172.16.5.33/dwhrscm_talend")
# sql_server = ce('mssql+pyodbc://andhi-ppi:Andhi2022!@172.16.19.36/DWH_RSCM?driver=SQL+Server+Native+Client+11.0', fast_executemany =True)
try :
    con_mysql = mysql.connect()
    conn_dwh = dwh.connect()
    # con_sqlserver = sql_server.connect()
    print('successfully connect to all database')
    
except SQLAlchemyError as e:
    error=str(e.__dict__['orig'])
    print(error)

# tarik data dari source 
source = pd.read_sql_query(""" SELECT
                                    a.prescription_id as PrescriptionID, -- char(15)
                                    b.item_id as SequenceID, -- int(11)
                                    a.patient_id as PatientID, -- int(10)
                                    a.admission_id as AdmissionID, -- int(10)
                                    a.presc_org_id as PrescriptionOrgID,
                                    b.obj_id as DrugID, -- char(20)
                                    b.obj_qty as DrugQuantity, -- char(30)
                                    b.freq_cd as FrequencyCode, -- varchar(255)
                                    b.unit_cd as UnitCode, -- char(15)
                                    b.x_qty as DispenseQuantity, -- char(30)
                                    b.x_unit_cd as DispenseUnitCode, -- char(15)
                                    a.created_dttm as InsertDateApp
                                FROM
                                xocp_ehr_prescription AS a
                                inner JOIN xocp_ehr_prescription_item AS b ON a.prescription_id = b.prescription_id
                                where admission_id <> '0' and 
                                -- a.created_dttm >= '2023-07-04 00:00:00' AND a.created_dttm <= '2023-07-04 23:59:59'
                                -- AND a.patient_id = 1678877 AND a.admission_id = 9
                                a.created_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 2 DAY), "%%Y-%%m-%%d 00:00:00") AND a.created_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 23:59:59")
        """, con_mysql)
print(source)
print(source.dtypes)


if source.empty:
    print('tidak ada data dari source')
else:
    # jika dari source cuma 1 row
    if len(source) == 1:        
        # ambil primary key dari source, ambil index ke 0
        prescriptionid = source["PrescriptionID"].values[0]
        sequenceid = source["SequenceID"].values[0]

        # query buat narik data dari target lalu filter berdasarkan primary key
        query = f'SELECT PrescriptionID,SequenceID,PatientID,AdmissionID,PrescriptionOrgID,DrugID,DrugQuantity,FrequencyCode,UnitCode,DispenseQuantity,DispenseUnitCode,InsertDateApp from FactDrugPrescription where PrescriptionID IN ({prescriptionid}) AND SequenceID IN ({sequenceid}) order by PrescriptionID,SequenceID'
        target = pd.read_sql_query(query, conn_dwh)
    else :
         # ambil primary key dari source, pake unique biar tidak duplicate
        prescriptionid = tuple(source["PrescriptionID"].unique())
        sequenceid = tuple(source["SequenceID"].unique())

         # query buat narik data dari target lalu filter berdasarkan primary key
        query = f'SELECT PrescriptionID,SequenceID,PatientID,AdmissionID,PrescriptionOrgID,DrugID,DrugQuantity,FrequencyCode,UnitCode,DispenseQuantity,DispenseUnitCode,InsertDateApp from FactDrugPrescription where PrescriptionID IN {prescriptionid} AND SequenceID IN {sequenceid} order by PrescriptionID,SequenceID'
        target = pd.read_sql_query(query, conn_dwh)
    print(target.dtypes)
    # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

    # ambil data yang update dari changes
    modified = changes[changes[['PrescriptionID','SequenceID']].apply(tuple,1).isin(target[['PrescriptionID','SequenceID']].apply(tuple,1))]
    total_row_upd = len(modified)
    text_upd = f'total row update : {total_row_upd}'
    print(text_upd)
    print(modified)

    # ambil data yang new dari changes
    inserted = changes[~changes[['PrescriptionID','SequenceID']].apply(tuple,1).isin(target[['PrescriptionID','SequenceID']].apply(tuple,1))]
    total_row_ins = len(inserted)
    text_ins = f'total row inserted : {total_row_ins}'
    print(text_ins)
    print(inserted)

    if modified.empty:
        # bikin tanggal sekarang buat kolom InsertDateDWH
        # today = dt.datetime.now()
        # today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
        # inserted['InsertDateDWH'] = today_convert
        inserted.to_sql('FactDrugPrescription', con=conn_dwh, if_exists = 'append', index=False)
        print('success insert all data without update')
    
    else:
        # buat fungsi untuk update data ke tabel target
        def updated_to_sql(df, table_name, key_1,key_2):
            list_col = []
            table=table_name
            pk_1 = key_1
            pk_2 = key_2
            temp_table = f'{table}_temporary_table'
            for col in df.columns:
                if col == pk_1 or col == pk_2 :
                    continue
                list_col.append(f'r.{col} = t.{col}')
            df.to_sql(temp_table,con=conn_dwh, if_exists='replace',index=False)
            update_stmt_1 = f'UPDATE {table} r '
            update_stmt_2 = f'INNER JOIN (SELECT * FROM {temp_table}) as t ON r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} '
            update_stmt_3 = f'SET '
            update_stmt_4 = ", ".join(list_col)
            update_stmt_5 = f' WHERE r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} '
            update_stmt_6 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_4 + update_stmt_5 +";"
            delete_stmt_1 = f'DROP TABLE {temp_table}'
            print(update_stmt_6)
            conn_dwh.execute(update_stmt_6)
            conn_dwh.execute(delete_stmt_1)

        try:
            # update data
            updated_to_sql(modified, 'FactDrugPrescription', 'PrescriptionID', 'SequenceID')

            # insert data baru
            # today = dt.datetime.now()
            # today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
            # inserted['InsertDateDWH'] = today_convert
            inserted.to_sql('FactDrugPrescription', con=conn_dwh, if_exists ='append',index=False)
            print('success update and insert all data')
        
        except Exception as e:
            print(e)


#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)

text=f'scheduler tanggal : {date}'
print(text)

con_mysql.close()
conn_dwh.close()
