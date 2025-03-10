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

sys.stdout = open("C:/TestPython/DWH_SQL_Server/DWH/logs/LogFactPatientForensic.txt","w")
t0 = time.time()

conn_his_live = db_connection.create_connection(db_connection.his_live)
conn_dwh_sqlserver = db_connection.create_connection(db_connection.dwh_sqlserver)

# query untuk menarik data dari patient_order lalu masukkan variabel date_range di dalam WHERE
query_source = f"""                            
                SELECT 
                    a.order_id OrderID,
                    a.patient_id PatientID,
                    a.admission_id AdmissionID, 
                    c.patient_mrn_txt MedicalNo,
                    d.admission_dttm AdmissionDate,
                    a.ordered_dttm OrderDate,
                    a.org_id OrgID,
                    e.org_nm as OrgName,
                    a.obj_id ObjID,
                    b.obj_nm ObjName, 
                    a.status_cd Status,
                    a.billing_paid BillingPaid
                FROM xocp_his_patient_order a
                LEFT JOIN xocp_obj b on a.obj_id = b.obj_id
                LEFT JOIN xocp_his_patient c on a.patient_id = c.patient_id
                LEFT JOIN xocp_his_patient_admission d on a.patient_id = d.patient_id and a.admission_id = d.admission_id
                LEFT JOIN xocp_orgs e on a.org_id = e.org_id
                WHERE a.org_id =429
                AND ordered_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 14 DAY), '%%Y-%%m-%%d 00:00:00') and ordered_dttm <= DATE_FORMAT(NOW(), '%%Y-%%m-%%d 23:59:59')
                """
source = pd.read_sql_query(query_source,conn_his_live)
print(source)

# jika data tarikan dari query kosong, maka tidak akan lanjut ke proses berikutnya
if source.empty:
    print('tidak ada data dari source')

# lalu kita bikin query untuk menarik data dari target dengan filter OrderID yang sudah ditarik dari source
else:
    # buat variabel orderid untuk menampung semua OrderID yang sudah ditarik di source
    orderid = tuple(source['OrderID'].unique())

    if len(orderid) > 1:
        pass
    else:
        orderid = str(orderid).replace(',','')

    query_target = f""" SELECT
                            OrderID,
                            PatientID,
                            AdmissionID,
                            MedicalNo,
                            AdmissionDate,
                            OrderDate,
                            OrgID,
                            OrgName,
                            ObjID,
                            ObjName,
                            Status,
                            BillingPaid
                        FROM dwhrscm_talend.FactPatientForensic
                        WHERE OrderID IN {orderid}
                    """
    target = pd.read_sql_query(query_target, conn_dwh_sqlserver)
    print(target)

    # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

    # ambil data yang update dari changes
    modified = changes[changes[['OrderID']].apply(tuple,1).isin(target[['OrderID']].apply(tuple,1))]
    total_row_upd = len(modified)
    text_upd = f'total row update : {total_row_upd}'
    print(text_upd)
    print(modified.iloc[:,0:6])

    # ambil data yang new dari changes
    inserted = changes[~changes[['OrderID']].apply(tuple,1).isin(target[['OrderID']].apply(tuple,1))]
    total_row_ins = len(inserted)
    text_ins = f'total row inserted : {total_row_ins}'
    print(text_ins)
    print(inserted.iloc[:,0:6])

    if modified.empty:
        inserted.to_sql('FactPatientForensic', schema = 'dwhrscm_talend',con = conn_dwh_sqlserver, if_exists = 'append', index=False)
        print('success insert all data without update')

    else:
        # buat fungsi untuk update data ke tabel target
        def updated_to_sql(df, table_name, key_1):
            list_col = []
            table=table_name
            pk_1 = key_1
            temp_table = f'{table}_temporary_table'
            for col in df.columns:
                if col == pk_1 :
                    continue
                list_col.append(f'r.{col} = t.{col}')
            df.to_sql(temp_table,schema='dwhrscm_talend',con=conn_dwh_sqlserver, if_exists='replace',index=False)
            update_stmt_1 = f'UPDATE r '
            update_stmt_2 = f'SET '
            update_stmt_3 = ", ".join(list_col)
            update_stmt_4 = f' FROM dwhrscm_talend.{table} r '
            update_stmt_5 = f'INNER JOIN (SELECT * FROM dwhrscm_talend.{temp_table}) as t ON r.{pk_1} = t.{pk_1} '
            update_stmt_6 = f' WHERE r.{pk_1} = t.{pk_1} '
            update_stmt_7 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_4 + update_stmt_5 + update_stmt_6 +";"
            delete_stmt_1 = f'DROP TABLE dwhrscm_talend.{temp_table}'
            print(update_stmt_7)
            conn_dwh_sqlserver.execute(update_stmt_7)
            conn_dwh_sqlserver.execute(delete_stmt_1)

        try:
            # update data
            updated_to_sql(modified, 'FactPatientForensic', 'OrderID')
            inserted.to_sql('FactPatientForensic', schema='dwhrscm_talend',con=conn_dwh_sqlserver, if_exists ='append',index=False)
            print('success update and insert all data')

        except Exception as e:
            print(e)

#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)

text=f'scheduler tanggal : {date}'
print(text)

# conn_ehr.close()
db_connection.close_connection(conn_his_live)
db_connection.close_connection(conn_dwh_sqlserver)