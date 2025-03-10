import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pyodbc
import pandas as pd
import numpy as np
import time
import datetime as dt
date = dt.datetime.today()

# bikin log ke file
sys.stdout = open("C:/TestPython/DWH_SQL_Server/DWH/logs/LogFactAppointmentQueueEHR.txt","w")
t0 = time.time()

# bikin koneksi ke db
conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)
conn_dwh_sqlserver = db_connection.create_connection(db_connection.dwh_sqlserver)

source = pd.read_sql_query(""" 
                            SELECT 
                                a.AppointmentID,
                                TRIM(a.KodeBooking) as KodeBooking,
                                a.AdmissionDate,
                                a.PatientID,
                                a.AdmissionID,
                                b.MedicalNo,
                                a.PatientType,
                                a.CardNo,
                                a.SEPNo,
                                a.ReferenceNo,
                                b.NIK,
                                b.PhoneNo,
                                a.PoliCode,
                                a.PoliName,
                                a.NewPatient,
                                TRIM(a.DoctorCode) AS DoctorCode,
                                a.DoctorName,
                                a.AppointmentDate,
                                a.AppointmentTime,
                                a.DoctorPracticeStartTime,
                                a.DoctorPracticeFinishTime,
                                a.VisitType,
                                a.AppointmentMethod,
                                a.QueueNumber,
                                a.QueueNo,
                                a.ServedEstimated,
                                a.RestQuotaJKN,
                                a.QuotaJKN,
                                a.RestQuotaNonJKN,
                                a.QuotaNonJKN,
                                a.Notes,
                                a.Flag 
                            FROM staging_rscm.TransAppointmentQueue a
                            LEFT JOIN staging_rscm.DimensionPatientMPI b ON a.PatientID = b.PatientID AND b.ScdActive = 1
                            WHERE Flag = 1    
                            -- AND (AdmissionDate >= '2023-06-13 00:00:00' AND AdmissionDate <= '2023-06-13 23:59:59')
                            -- AND CAST(InsertDateStaging as date) ='2024-04-26'
                            AND (CAST(InsertDateStaging as date) >= CAST(DATEADD(DAY, -2, GETDATE()) as date) AND CAST(InsertDateStaging as date) <= CAST(GETDATE() as date) 
                            OR CAST(UpdateDateStaging as date) >= CAST(DATEADD(DAY, -1, GETDATE()) as date) AND CAST(UpdateDateStaging as date) <= CAST(GETDATE() as date))                                -- AND AppointmentID IN (13551364,13551358)
                                """, con=conn_staging_sqlserver)

print(source)

# cek tipe data source
print(source.dtypes)

if source.empty:
    print('tidak ada data dari source')
else:
    # jika dari source cuma 1 row
    if len(source) == 1:        
        # ambil primary key dari source, ambil index ke 0
        appointmentid = source["AppointmentID"].values[0]
        patientid = source["PatientID"].values[0]
        admissionid = source["AdmissionID"].values[0]

        # query buat narik data dari target lalu filter berdasarkan primary key
        query = f"SELECT AppointmentID,TRIM(KodeBooking) as KodeBooking,AdmissionDate,PatientID,AdmissionID,MedicalNo,PatientType,CardNo,SEPNo,ReferenceNo,NIK,PhoneNo,PoliCode,PoliName, NewPatient,TRIM(DoctorCode) as DoctorCode,DoctorName,AppointmentDate,AppointmentTime,DoctorPracticeStartTime,DoctorPracticeFinishTime,VisitType,AppointmentMethod,ReferenceNo,QueueNumber,QueueNo,ServedEstimated,RestQuotaJKN,QuotaJKN,RestQuotaNonJKN,QuotaNonJKN,Notes,Flag FROM dwhrscm_talend.FactAppointmentQueue where AppointmentID IN ({appointmentid}) AND PatientID IN ({patientid}) AND AdmissionID IN ({admissionid}) AND Flag = '1' order by AppointmentID"
        target = pd.read_sql_query(query, conn_dwh_sqlserver)
    else :
         # ambil primary key dari source, pake unique biar tidak duplicate
        appointmentid = tuple(source["AppointmentID"].unique())
        patientid = tuple(source["PatientID"].unique())
        admissionid = tuple(source["AdmissionID"].unique())

         # query buat narik data dari target lalu filter berdasarkan primary key
        query = f"SELECT AppointmentID,TRIM(KodeBooking) as KodeBooking,AdmissionDate,PatientID,AdmissionID,MedicalNo,PatientType,CardNo,SEPNo,ReferenceNo,NIK,PhoneNo,PoliCode,PoliName, NewPatient,TRIM(DoctorCode) as DoctorCode,DoctorName,AppointmentDate,AppointmentTime,DoctorPracticeStartTime,DoctorPracticeFinishTime,VisitType,AppointmentMethod,QueueNumber,QueueNo,ServedEstimated,RestQuotaJKN,QuotaJKN,RestQuotaNonJKN,QuotaNonJKN,Notes,Flag FROM dwhrscm_talend.FactAppointmentQueue where AppointmentID IN {appointmentid} AND PatientID IN {patientid} AND AdmissionID IN {admissionid} and Flag = '1' order by AppointmentID"
        target = pd.read_sql_query(query, conn_dwh_sqlserver)
        
    # cek tipe data target
    print(target.dtypes)

    # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]
    
    # ambil data yang update dari changes
    modified = changes[changes[['AppointmentID','PatientID','AdmissionID','Flag']].apply(tuple,1).isin(target[['AppointmentID','PatientID','AdmissionID','Flag']].apply(tuple,1))]
    total_row_upd = len(modified)
    text_upd = f'total row update : {total_row_upd}'
    print(text_upd)
    print(modified)

    # ambil data yang new dari changes
    inserted = changes[~changes[['AppointmentID','PatientID','AdmissionID','Flag']].apply(tuple,1).isin(target[['AppointmentID','PatientID','AdmissionID','Flag']].apply(tuple,1))]
    total_row_ins = len(inserted)
    text_ins = f'total row inserted : {total_row_ins}'
    print(text_ins)
    print(inserted)

    # print(source.iloc[:,0:3].apply(tuple,1))
    # print(target.iloc[:,0:3].apply(tuple,1))
    # print(source.iloc[:,2:4].apply(tuple,1))
    # print(target.iloc[:,2:4].apply(tuple,1))
    # print(source.iloc[:,3:5].apply(tuple,1))
    # print(target.iloc[:,3:5].apply(tuple,1))
    # print(source.iloc[:,4:6].apply(tuple,1))
    # print(target.iloc[:,4:6].apply(tuple,1))
    # print(source.iloc[:,5:7].apply(tuple,1))
    # print(target.iloc[:,5:7].apply(tuple,1))
    # print('bates')
    # print(source.iloc[:,7:9].apply(tuple,1))
    # print(target.iloc[:,7:9].apply(tuple,1))
    # print(source.iloc[:,9:11].apply(tuple,1))
    # print(target.iloc[:,9:11].apply(tuple,1))
    # print(source.iloc[:,11:13].apply(tuple,1))
    # print(target.iloc[:,11:13].apply(tuple,1))
    # print(source.iloc[:,13:15].apply(tuple,1))
    # print(target.iloc[:,13:15].apply(tuple,1))
    # print(source.iloc[:,15:17].apply(tuple,1))
    # print(target.iloc[:,15:17].apply(tuple,1))
    # print(source.iloc[:,17:20].apply(tuple,1))
    # print(target.iloc[:,17:20].apply(tuple,1))
    # print(source.iloc[:,20:24].apply(tuple,1))
    # print(target.iloc[:,20:24].apply(tuple,1))
    # print('bates lagi')
    # print(source.iloc[:,21:25].apply(tuple,1))
    # print(target.iloc[:,21:25].apply(tuple,1))
    # print(source.iloc[:,25:31].apply(tuple,1))
    # print(target.iloc[:,25:31].apply(tuple,1))

    if modified.empty:
        # bikin tanggal sekarang buat kolom InsertedDateDWH
        today = dt.datetime.now()
        today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
        inserted['InsertedDateDWH'] = today_convert
        inserted.to_sql('FactAppointmentQueue', schema='dwhrscm_talend', con = conn_dwh_sqlserver, if_exists = 'append', index=False)
        print('success insert all data without update')
    
    else:
        # buat fungsi untuk update data ke tabel target
        def updated_to_sql(df, table_name, key_1,key_2,key_3,key_4):
            list_col = []
            table=table_name
            pk_1 = key_1
            pk_2 = key_2
            pk_3 = key_3
            pk_4 = key_4
            temp_table = f'{table}_temporary_table'
            for col in df.columns:
                if col == pk_1 or col == pk_2 or col == pk_3 or col == pk_4:
                    continue
                list_col.append(f'r.{col} = t.{col}')
            df.to_sql(temp_table,schema = 'dwhrscm_talend',con=conn_dwh_sqlserver, if_exists='replace',index=False)
            update_stmt_1 = f'UPDATE r '
            update_stmt_2 = f'SET '
            update_stmt_3 = ", ".join(list_col)
            update_stmt_8 = f' , r.UpdateDateDWH = CONVERT(DATETIME2(0), GETDATE(),120)'
            update_stmt_9 = f' , r.IsSent = 0 '
            update_stmt_10 = f' , r.IsSentBPJS = 0 '
            update_stmt_4 = f' FROM dwhrscm_talend.{table} r '
            update_stmt_5 = f'INNER JOIN (SELECT * FROM dwhrscm_talend.{temp_table}) as t ON r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} AND r.{pk_3} = t.{pk_3} AND r.{pk_4} = t.{pk_4} '
            update_stmt_6 = f'WHERE r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} AND r.{pk_3} = t.{pk_3} AND r.{pk_4} = t.{pk_4} '
            update_stmt_7 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_8 + update_stmt_9 + update_stmt_10 + update_stmt_4 + update_stmt_5 + update_stmt_6 +";"
            delete_stmt_1 = f'DROP TABLE dwhrscm_talend.{temp_table}'
            print(update_stmt_7)
            conn_dwh_sqlserver.execute(update_stmt_7)
            conn_dwh_sqlserver.execute(delete_stmt_1)

        try:
            # update data
            updated_to_sql(modified, 'FactAppointmentQueue', 'AppointmentID','PatientID','AdmissionID','Flag')

            # insert data baru
            today = dt.datetime.now()
            today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
            inserted['InsertedDateDWH'] = today_convert
            inserted.to_sql('FactAppointmentQueue', schema='dwhrscm_talend', con=conn_dwh_sqlserver, if_exists ='append',index=False)
            print('success update and insert all data')
        
        except Exception as e:
            print(e)

#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)

text=f'scheduler tanggal : {date}'
print(text)

db_connection.close_connection(conn_dwh_sqlserver)
db_connection.close_connection(conn_staging_sqlserver)