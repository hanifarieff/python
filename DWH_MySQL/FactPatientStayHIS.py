import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pandas as pd
import pyodbc
import datetime as dt
date = dt.datetime.today()
import time

# bikin log ke file
sys.stdout = open("C:/TestPython/DWH_MySQL/logs/LogFactPatientStayHIS.txt","w")
text=f'scheduler tanggal : {date}'
print(text)
t0 = time.time()

# bikin koneksi ke db
conn_his = db_connection.create_connection(db_connection.replika_his)
conn_ehr = db_connection.create_connection(db_connection.replika_ehr)
conn_dwh_mysql = db_connection.create_connection(db_connection.dwh_mysql)

# tarik query, masuk ke variabel source
source = pd.read_sql_query(""" SELECT DISTINCT
                                    adm.patient_id as PatientID,
                                    adm.admission_id as AdmissionID,
                                    p.patient_mrn_txt AS MedicalNo,
                                    adm.admission_dttm as AdmissionDate,
                                    CASE
                                        WHEN x.patient_id IS NULL THEN 'n'
                                        ELSE 'y'
                                    END AS StayInd
                                FROM xocp_his_patient_admission adm
                                LEFT JOIN xocp_his_patient p ON p.patient_id = adm.patient_id
                                LEFT JOIN 
                                (
                                    SELECT 
                                        a.patient_id,
                                        a.admission_id,
                                        a.location_id,
                                        a.created_dttm,
                                        a.updated_dttm
                                    FROM xocp_his_patient_location a 
                                    LEFT JOIN xocp_location b on a.location_id = b.location_id 
                                    WHERE a.status_cd NOT IN ('nullified') AND b.sk_count = '1'
                                -- 	AND a.patient_id = 994340 and a.admission_id = 3
                                    ORDER BY patient_id,admission_id
                                ) x ON x.patient_id = adm.patient_id and x.admission_id = adm.admission_id
                                WHERE adm.org_id in (select org_id from xocp_orgs where parent_id in ('687','1872','2418') OR org_id in ('687','1872','2418'))
                                AND adm.status_cd NOT IN ('nullified','cancelled')
                                -- AND adm.admission_dttm >= '2023-09-17 00:00:00' AND adm.admission_dttm <= '2023-09-17 23:59:59'
                                AND ((adm.admission_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 2 DAY), "%%Y-%%m-%%d 00:00:00") AND adm.admission_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 23:59:59"))
                                OR (x.created_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 2 DAY), "%%Y-%%m-%%d 00:00:00") AND x.created_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 23:59:59"))
                                OR (x.updated_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 2 DAY), "%%Y-%%m-%%d 00:00:00") AND x.updated_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 23:59:59")))
                            """, conn_his)
source['Flag'] = '2'
print(source)

if source.empty:
    print('tidak ada data dari source')
else:
    # jika dari source cuma 1 row
    if len(source) == 1:        
        # ambil primary key dari source, ambil index ke 0
        patientid = source["PatientID"].values[0]
        admissionid = source["AdmissionID"].values[0]
        # query buat narik data dari target lalu filter berdasarkan primary key
        query = f'SELECT PatientID,AdmissionID,MedicalNo,AdmissionDate,StayInd,Flag from FactPatientStay where PatientID IN ({patientid})  AND AdmissionID IN ({admissionid}) AND Flag = 2 order by PatientID, AdmissionID'
        target = pd.read_sql_query(query, conn_dwh_mysql)
    else :
        # ambil key dari source
        patientid = tuple(source["PatientID"]) 
        admissionid = tuple(source["AdmissionID"])

        # query buat narik data dari target lalu filter berdasarkan primary key
        query = f'SELECT PatientID,AdmissionID,MedicalNo,AdmissionDate,StayInd,Flag from FactPatientStay where PatientID IN {patientid} AND AdmissionID IN {admissionid} AND Flag = 2 order by PatientID, AdmissionID'
        target = pd.read_sql_query(query, conn_dwh_mysql)
    
    print(target)
# ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

# ambil data yang update
modified = changes[changes[['PatientID','AdmissionID','Flag']].apply(tuple,1).isin(target[['PatientID','AdmissionID','Flag']].apply(tuple,1))]
total_row_upd = len(modified)
text_upd = f'total row update : {total_row_upd}'
print(text_upd)
print(modified)

#ambil data yang baru
inserted = changes[~changes[['PatientID','AdmissionID','Flag']].apply(tuple,1).isin(target[['PatientID','AdmissionID','Flag']].apply(tuple,1))]
total_row_ins = len(inserted)
text_ins = f'total row inserted : {total_row_ins}'
print(text_ins)
print(inserted)

if modified.empty:
    inserted.to_sql('FactPatientStay', con = conn_dwh_mysql, if_exists='append',index=False)
    print('success insert without update')
else:
    # buat fungsi untuk update
    def updated_to_sql(df, table_name, key_1, key_2, key_3):
        list_col = []
        table = table_name
        pk_1 = key_1
        pk_2 = key_2
        pk_3 = key_3
        temp_table = f'{table}_temporary_table'
        for col in df.columns:
            if col == pk_1 or col == pk_2 or col == pk_3:
                continue
            list_col.append(f't.{col} = s.{col}')
        df.to_sql(temp_table, con = conn_dwh_mysql, if_exists='replace',index = False)
        update_stmt_1 = f'UPDATE {table} t '
        update_stmt_2 = f'INNER JOIN (SELECT * from {temp_table}) AS s ON t.{pk_1} = s.{pk_1} AND t.{pk_2} = s.{pk_2} AND t.{pk_3} = s.{pk_3} '
        update_stmt_3 = f'SET '
        update_stmt_4 = ", ".join(list_col)
        update_stmt_5 = f' WHERE t.{pk_1} = s.{pk_1} AND t.{pk_2} = s.{pk_2} AND t.{pk_3} = s.{pk_3}'
        update_stmt_6 = update_stmt_1 + update_stmt_2 + update_stmt_3  + update_stmt_4 + update_stmt_5 +";"
        delete_stmt_1 = f'DROP TABLE {temp_table}'
        print(update_stmt_6)
        print('\n')
        conn_dwh_mysql.execute(update_stmt_6)
        conn_dwh_mysql.execute(delete_stmt_1)

    try:
        # call fungsi update
        updated_to_sql(modified, 'FactPatientStay', 'PatientID','AdmissionID','Flag')
        # masukkan data yang baru ke table target
        inserted.to_sql('FactPatientStay', con = conn_dwh_mysql, if_exists='append',index=False)
        print('success update dan insert')
    except Exception as e:
        print(e)

t1 = time.time()
total=t1-t0
print(total)

db_connection.close_connection(conn_ehr)
db_connection.close_connection(conn_dwh_mysql)
db_connection.close_connection(conn_his)
sys.stdout.close()