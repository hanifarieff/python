from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine
import pandas as pd
import time
import datetime as dt
date = dt.datetime.today()
import sys

sys.stdout = open("C:/TestPython/DWH_MySQL/logs/LogFactPatientLabObs.txt","w")

t0 = time.time()
ehr = create_engine('mysql://hanif-ppi:hanif2022@172.16.19.11/ehr')
dwh_talend = create_engine('mysql://hanif-ppi:hanif2022@172.16.5.33/dwhrscm_talend')

try:
    conn_ehr = ehr.connect()
    conn_dwh = dwh_talend.connect()
    print('successfully connect DB')
except SQLAlchemyError as e:
    error = str(e.__dict__['orig'])
    print(error)

source = pd.read_sql_query(""" 
                                SELECT
                                    a.patient_id as PatientID,
                                    a.admission_id as AdmissionID,
                                    b.mrn as MedicalNo,
                                    SUBSTRING_INDEX(b.order_id,'^',1) as OrderID,
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(b.order_id,'^',2),'^',-1) as OrderCodeID,
                                    SUBSTRING_INDEX(b.order_id,'^',-1) as OrderNameID,
                                    a.order_nm as OrderName,
                                    a.order_req_dttm as OrderRequestDate,
                                    b.obs_id as ObservationID,
                                    b.obs_name as ObservationName,
                                    b.obs_value as ObservationValue,
                                    b.obs_unit as ObservationUnit,
                                    b.abnormal_flag as AbnormalFlag,
                                    b.ref_range as RefRange,
                                    b.obs_notes as ObservationNotes,
                                    b.result_dttm as ResultDate,
                                    b.responsible_nm as DoctorResponsible,
                                    b.created_dttm as CreatedDate,
                                    b.status_cd as StatusCode
                                FROM obs_header a
                                INNER JOIN obs_item b 
                                ON a.order_id = SUBSTRING_INDEX(b.order_id, '^',2) and a.patient_id = b.patient_id and a.admission_id = b.admission_id
                                where ((a.order_req_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 3 DAY), "%%Y-%%m-%%d 00:00:00") AND a.order_req_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 23:59:59")) OR
                                (b.created_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 3 DAY), "%%Y-%%m-%%d 00:00:00") AND b.created_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 23:59:59")))
                                ORDER BY a.patient_id, a.admission_id """, conn_ehr)
print(source.shape)

# ambil key dari source
patientid = tuple(source["PatientID"].unique()) 
admissionid = tuple(source["AdmissionID"].unique())
orderid = tuple(source["OrderID"].unique())
ordercodeid = tuple(source["OrderCodeID"].unique())
ordernameid = tuple(source["OrderNameID"].unique())
observationid = tuple(source["ObservationID"].unique())

# query buat narik data dari target lalu filter berdasarkan primary key
query = f'SELECT PatientID,AdmissionID,MedicalNo, OrderID,OrderCodeID,OrderNameID,OrderName,OrderRequestDate,ObservationID,ObservationName,ObservationValue,ObservationUnit,AbnormalFlag,RefRange,ObservationNotes,ResultDate,DoctorResponsible,CreatedDate,StatusCode from FactPatientLabObs where PatientID IN {patientid} AND AdmissionID IN {admissionid} AND OrderID IN {orderid} AND OrderCodeID IN {ordercodeid} AND OrderNameID IN {ordernameid} AND ObservationID IN {observationid} order by PatientID, AdmissionID'
target = pd.read_sql_query(query, conn_dwh)

# ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]
# print(changes)

# ambil data yang update
modified = changes[changes[['PatientID','AdmissionID','OrderID','OrderCodeID','OrderNameID','ObservationID']].apply(tuple,1).isin(target[['PatientID','AdmissionID','OrderID','OrderCodeID','OrderNameID','ObservationID']].apply(tuple,1))]
total_row_upd = len(modified)
text_upd = f'total row update : {total_row_upd}'
print(text_upd)
print(modified.iloc[:,:8])

#ambil data yang baru
inserted = changes[~changes[['PatientID','AdmissionID','OrderID','OrderCodeID','OrderNameID','ObservationID']].apply(tuple,1).isin(target[['PatientID','AdmissionID','OrderID','OrderCodeID','OrderNameID','ObservationID']].apply(tuple,1))]
total_row_ins = len(inserted)
text_ins = f'total row inserted : {total_row_ins}'
print(text_ins)
print(inserted.iloc[:,:8])

if modified.empty:
    inserted.to_sql('FactPatientLabObs', con=conn_dwh, if_exists = 'append', index=False)
    print('success insert without update')
else:
    # buat fungsi update data
    def updated_data(df, table_name, key_1,key_2,key_3,key_4,key_5,key_6):
        a = []
        table = table_name
        pk_1 = key_1
        pk_2 = key_2
        pk_3 = key_3
        pk_4 = key_4
        pk_5 = key_5
        pk_6 = key_6
        temp_table = f'{table}_temporary_table'
        for col in df.columns:
            if col == pk_1 or col == pk_2 or col == pk_3 or col == pk_4 or col == pk_5 or col == pk_6:
                continue
            a.append(f't.{col} = s.{col}')
        df.to_sql(temp_table, conn_dwh, if_exists = 'replace', index = False)
        update_stmt_1 = f'UPDATE {table} t '
        update_stmt_2 = f'INNER JOIN (SELECT * FROM {temp_table}) AS s ON t.{pk_1} = s.{pk_1} AND t.{pk_2} = s.{pk_2} AND t.{pk_3} = s.{pk_3} AND t.{pk_4} = s.{pk_4} AND t.{pk_5} = s.{pk_5} AND t.{pk_6} = s.{pk_6} '
        update_stmt_3 = f'SET '
        update_stmt_4 = ", ".join(a)
        update_stmt_5 =  f' WHERE t.{pk_1} = s.{pk_1} AND t.{pk_2} = s.{pk_2} AND t.{pk_3} = s.{pk_3} AND t.{pk_4} = s.{pk_4} AND t.{pk_5} = s.{pk_5} AND t.{pk_6} = s.{pk_6}'
        update_stmt_6 = update_stmt_1 +  update_stmt_2 + update_stmt_3 + update_stmt_4 + update_stmt_5 + ";"
        delete_stmt_1 = f'DROP TABLE {temp_table} '
        print(update_stmt_6)
        conn_dwh.execute(update_stmt_6)
        conn_dwh.execute(delete_stmt_1)

    try:
        #update data
        updated_data(modified, 'FactPatientLabObs', 'PatientID', 'AdmissionID','OrderID','OrderCodeID','OrderNameID','ObservationID')

        #insert data
        inserted.to_sql('FactPatientLabObs', con=conn_dwh, if_exists = 'append', index=False)

        print('all success updated and inserted')
    except Exception as e:
        print(e)

#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)
text=f'scheduler tanggal : {date}'
print(text)

conn_dwh.close()
conn_ehr.close()
sys.stdout.close()

