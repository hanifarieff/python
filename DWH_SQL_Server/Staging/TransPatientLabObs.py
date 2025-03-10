import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pandas as pd
import pyodbc
import datetime as dt
date = dt.datetime.today()
import time

# bikin log ke file
sys.stdout = open("C:/TestPython/DWH_SQL_Server/Staging/logs/LogTransPatientLabObs.txt","w")

t0 = time.time()

# bikin koneksi ke db
conn_ehr = db_connection.create_connection(db_connection.replika_ehr)
conn_ehr_live = db_connection.create_connection(db_connection.ehr_live)
conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)

source = pd.read_sql_query(""" 
                              SELECT
                                    a.patient_id as PatientID,
                                    a.admission_id as AdmissionID,
                                    -- b.mrn as MedicalNo,
                                    SUBSTRING_INDEX(b.order_id,'^',1) as OrderLab,
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
                                -- WHERE (a.order_req_dttm >= '2023-02-18 00:00:00' AND a.order_req_dttm <= '2023-02-19 23:59:59')
                                where 
                                -- a.patient_id =1842352 and a.admission_id = 10
                                (a.order_req_dttm >= '2025-03-07 00:00:00' AND a.order_req_dttm <= '2025-03-08 23:59:59')
                                -- (a.order_req_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 4 DAY), "%%Y-%%m-%%d 00:00:00") AND a.order_req_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 2 DAY), "%%Y-%%m-%%d 23:59:59")) 
                                -- OR
                                -- (b.updated_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 00:00:00") AND b.updated_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 23:59:59"))
                                -- b.created_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 00:00:00") AND b.created_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 23:59:59")
                                ORDER BY a.patient_id, a.admission_id """, conn_ehr)
print(source.iloc[:,[0,1,2,3,7]])

if source.empty:
    print('tidak ada data dari source')
else:
    # ambil key dari source
    patientid = tuple(source["PatientID"].unique()) 
    admissionid = tuple(source["AdmissionID"].unique())
    orderlab = tuple(source["OrderLab"].unique())
    ordercodeid = tuple(source["OrderCodeID"].unique())
    ordernameid = tuple(source["OrderNameID"].unique())
    observationid = tuple(source["ObservationID"].unique())

    # bikin function remove comma jika ada variable yang isinya cuma 1, variable ini akan di pakai IN CLAUSE di query target
    def remove_comma(x):
        if len(x) == 1:
            return str(x).replace(',','')
        else:
            return x
    
    patientid = remove_comma(patientid)
    admissionid = remove_comma(admissionid)
    orderlab = remove_comma(orderlab)
    ordercodeid = remove_comma(ordercodeid)
    ordernameid = remove_comma(ordernameid)
    observationid = remove_comma(observationid)

# query buat narik data dari target lalu filter berdasarkan primary key
query = f'SELECT PatientID,AdmissionID, OrderLab,OrderCodeID,OrderNameID,OrderName,OrderRequestDate,ObservationID,ObservationName,ObservationValue,ObservationUnit,AbnormalFlag,RefRange,ObservationNotes,ResultDate,DoctorResponsible,CreatedDate,StatusCode from staging_rscm.TransPatientLabObs where PatientID IN {patientid} AND AdmissionID IN {admissionid} AND OrderLab IN {orderlab} AND OrderCodeID IN {ordercodeid} AND OrderNameID IN {ordernameid} AND ObservationID IN {observationid} order by PatientID, AdmissionID'
target = pd.read_sql_query(query, conn_staging_sqlserver)

# ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

# ambil data yang update
modified = changes[changes[['PatientID','AdmissionID','OrderLab','OrderCodeID','OrderNameID','ObservationID']].apply(tuple,1).isin(target[['PatientID','AdmissionID','OrderLab','OrderCodeID','OrderNameID','ObservationID']].apply(tuple,1))]
total_row_upd = len(modified)
text_upd = f'total row update : {total_row_upd}'
print(text_upd)
print(modified.iloc[:,[0,1,2,3,7]])

#ambil data yang baru
inserted = changes[~changes[['PatientID','AdmissionID','OrderLab','OrderCodeID','OrderNameID','ObservationID']].apply(tuple,1).isin(target[['PatientID','AdmissionID','OrderLab','OrderCodeID','OrderNameID','ObservationID']].apply(tuple,1))]
total_row_ins = len(inserted)
text_ins = f'total row inserted : {total_row_ins}'
print(text_ins)
print(inserted.iloc[:,[0,1,2,3,7]])

if modified.empty:
    inserted.to_sql('TransPatientLabObs', schema='staging_rscm' ,con=conn_staging_sqlserver, if_exists = 'append', index=False)
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
        df.to_sql(temp_table, schema='staging_rscm', con=conn_staging_sqlserver, if_exists = 'replace', index = False)
        update_stmt_1 = f'UPDATE t '
        update_stmt_2 = f'SET '
        update_stmt_3 = ", ".join(a)
        update_stmt_8 = f' , t.UpdateDateDWH = CONVERT(DATETIME2(0), GETDATE(),120)'
        update_stmt_4 = f' FROM staging_rscm.{table} t '
        update_stmt_5 = f'INNER JOIN (SELECT * FROM staging_rscm.{temp_table}) AS s ON t.{pk_1} = s.{pk_1} AND t.{pk_2} = s.{pk_2} AND t.{pk_3} = s.{pk_3} AND t.{pk_4} = s.{pk_4} AND t.{pk_5} = s.{pk_5} AND t.{pk_6} = s.{pk_6} '
        update_stmt_6 = f' WHERE t.{pk_1} = s.{pk_1} AND t.{pk_2} = s.{pk_2} AND t.{pk_3} = s.{pk_3} AND t.{pk_4} = s.{pk_4} AND t.{pk_5} = s.{pk_5} AND t.{pk_6} = s.{pk_6}'
        update_stmt_7 = update_stmt_1 +  update_stmt_2 + update_stmt_3 + update_stmt_8 + update_stmt_4 + update_stmt_5 + update_stmt_6 +";"
        delete_stmt_1 = f'DROP TABLE staging_rscm.{temp_table} '
        print(update_stmt_7)
        conn_staging_sqlserver.execute(update_stmt_7)
        conn_staging_sqlserver.execute(delete_stmt_1)

    try:
        #update data
        updated_data(modified, 'TransPatientLabObs', 'PatientID', 'AdmissionID','OrderLab','OrderCodeID','OrderNameID','ObservationID')

        #insert data
        inserted.to_sql('TransPatientLabObs', schema='staging_rscm', con=conn_staging_sqlserver, if_exists = 'append', index=False)

        print('all success updated and inserted')
    except Exception as e:
        print(e)

#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)
print('\n')
text=f'scheduler tanggal : {date}'
print(text)

db_connection.close_connection(conn_ehr)
db_connection.close_connection(conn_ehr_live)
db_connection.close_connection(conn_staging_sqlserver)
sys.stdout.close()

