import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pandas as pd
import pyodbc
import time
import datetime as dt
date = dt.datetime.today()

# bikin log ke file
sys.stdout = open("C:/TestPython/DWH_SQL_Server/DWH/logs/LogFactPatientLabObs.txt","w")

t0 = time.time()

# bikin koneksi ke db
conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)
conn_dwh_sqlserver = db_connection.create_connection(db_connection.dwh_sqlserver)

source = pd.read_sql_query(""" 
                                SELECT 
                                    b.PatientSurrogateKey as PatientSurrogateKeyID,
                                    a.PatientID,
                                    a.AdmissionID,
                                    b.MedicalNo, 
                                    a.OrderLab,
                                    a.OrderCodeID,
                                    a.OrderNameID,
                                    a.OrderName,
                                    a.OrderRequestDate,
                                    a.ObservationID,
                                    a.ObservationName,
                                    a.ObservationValue,
                                    a.ObservationUnit,
                                    a.AbnormalFlag,
                                    a.RefRange,
                                    a.ObservationNotes,
                                    a.ResultDate,
                                    a.DoctorResponsible,
                                    a.CreatedDate,
                                    a.StatusCode  
                                FROM staging_rscm.TransPatientLabObs a
                                LEFT JOIN staging_rscm.DimensionPatientMPI b ON a.PatientID = b.PatientID AND b.ScdActive = 1
                                -- where a.OrderRequestDate >= '2024-03- 00:00:00' AND a.OrderRequestDate <= '2024-03-08 23:59:59'
                                -- (CAST(a.CreatedDate as date) >= '2023-04-12' AND CAST(a.CreatedDate as date) <= '2023-04-17')
                                WHERE 
                                -- CAST(a.OrderRequestDate as date) >= '2024-01-17' AND CAST(a.OrderRequestDate as date) <= '2024-01-20'
                                (CAST(a.InsertDateStaging as date) >= CAST(DATEADD(DAY, -1, GETDATE()) as date) AND CAST(a.InsertDateStaging as date) <= CAST(GETDATE() as date)) 
                                OR
                                (CAST(a.UpdateDateDWH as date) >= CAST(DATEADD(DAY, -2, GETDATE()) as date) AND CAST(a.UpdateDateDWH as date) <= CAST(GETDATE() as date))
                                -- CAST(a.CreatedDate as date) = CAST(DATEADD(DAY, -1, GETDATE()) as date)
                                AND b.MedicalNo NOT IN (SELECT MedicalNo FROM staging_rscm.DimensionDummyPatient)
                                ORDER BY a.PatientID, a.AdmissionID
""", conn_staging_sqlserver)
print(source.iloc[:,[0,1,2,3,7,17]])

# ambil key dari source
patientid = tuple(source["PatientID"].unique()) 
admissionid = tuple(source["AdmissionID"].unique())
orderlab = tuple(source["OrderLab"].unique())
ordercodeid = tuple(source["OrderCodeID"].unique())
ordernameid = tuple(source["OrderNameID"].unique())
observationid = tuple(source["ObservationID"].unique())

# query buat narik data dari target lalu filter berdasarkan primary key
query = f'SELECT PatientSurrogateKeyID,PatientID,AdmissionID,MedicalNo, OrderLab,OrderCodeID,OrderNameID,OrderName,OrderRequestDate,ObservationID,ObservationName,ObservationValue,ObservationUnit,AbnormalFlag,RefRange,ObservationNotes,ResultDate,DoctorResponsible,CreatedDate,StatusCode from dwhrscm_talend.FactPatientLabObs where PatientID IN {patientid} AND AdmissionID IN {admissionid} AND OrderLab IN {orderlab} AND OrderCodeID IN {ordercodeid} AND OrderNameID IN {ordernameid} AND ObservationID IN {observationid} order by PatientID, AdmissionID'
target = pd.read_sql_query(query, conn_dwh_sqlserver)
# print(target)

# ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

# ambil data yang update
modified = changes[changes[['PatientID','AdmissionID','OrderLab','OrderCodeID','OrderNameID','ObservationID']].apply(tuple,1).isin(target[['PatientID','AdmissionID','OrderLab','OrderCodeID','OrderNameID','ObservationID']].apply(tuple,1))]
total_row_upd = len(modified)
text_upd = f'total row update : {total_row_upd}'
print(text_upd)
print(modified.iloc[:,[0,1,2,3,7,17]])

#ambil data yang baru
inserted = changes[~changes[['PatientID','AdmissionID','OrderLab','OrderCodeID','OrderNameID','ObservationID']].apply(tuple,1).isin(target[['PatientID','AdmissionID','OrderLab','OrderCodeID','OrderNameID','ObservationID']].apply(tuple,1))]
total_row_ins = len(inserted)
text_ins = f'total row inserted : {total_row_ins}'
print(text_ins)
print(inserted.iloc[:,[0,1,2,3,7,17]])

if modified.empty:
    today = dt.datetime.now()
    today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
    inserted['InsertedDateDWH'] = today_convert
    inserted.to_sql('FactPatientLabObs',schema='dwhrscm_talend', con=conn_dwh_sqlserver, if_exists = 'append', index=False)
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
        df.to_sql(temp_table, schema='dwhrscm_talend', con=conn_dwh_sqlserver, if_exists = 'replace', index = False)
        update_stmt_1 = f'UPDATE t '
        update_stmt_2 = f'SET '
        update_stmt_3 = ", ".join(a)
        update_stmt_8 = f' , t.UpdateDateDWH = CONVERT(DATETIME2(0), GETDATE(),120)'
        update_stmt_4 = f' FROM dwhrscm_talend.{table} t '
        update_stmt_5 = f'INNER JOIN (SELECT * FROM dwhrscm_talend.{temp_table}) AS s ON t.{pk_1} = s.{pk_1} AND t.{pk_2} = s.{pk_2} AND t.{pk_3} = s.{pk_3} AND t.{pk_4} = s.{pk_4} AND t.{pk_5} = s.{pk_5} AND t.{pk_6} = s.{pk_6} '
        update_stmt_6 = f' WHERE t.{pk_1} = s.{pk_1} AND t.{pk_2} = s.{pk_2} AND t.{pk_3} = s.{pk_3} AND t.{pk_4} = s.{pk_4} AND t.{pk_5} = s.{pk_5} AND t.{pk_6} = s.{pk_6}'
        update_stmt_7 = update_stmt_1 +  update_stmt_2 + update_stmt_3 + update_stmt_8 + update_stmt_4 + update_stmt_5 + update_stmt_6 +";"
        delete_stmt_1 = f'DROP TABLE dwhrscm_talend.{temp_table} '
        print(update_stmt_7)
        conn_dwh_sqlserver.execute(update_stmt_7)
        conn_dwh_sqlserver.execute(delete_stmt_1)

    try:
        #update data
        updated_data(modified, 'FactPatientLabObs', 'PatientID', 'AdmissionID','OrderLab','OrderCodeID','OrderNameID','ObservationID')

        #insert data
        today = dt.datetime.now()
        today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
        inserted['InsertedDateDWH'] = today_convert
        inserted.to_sql('FactPatientLabObs', schema='dwhrscm_talend', con=conn_dwh_sqlserver, if_exists = 'append', index=False)

        print('all success updated and inserted')
    except Exception as e:
        print(e)

#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)

text=f'scheduler tanggal : {date}'
print(text)

db_connection.close_connection(conn_staging_sqlserver)
db_connection.close_connection(conn_dwh_sqlserver)
sys.stdout.close()

