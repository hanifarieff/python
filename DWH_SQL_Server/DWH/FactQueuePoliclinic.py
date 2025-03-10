import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pyodbc
import pandas as pd
import time
import datetime as dt
import sys

#bikin log ke file
sys.stdout = open("C:/TestPython/DWH_SQL_Server/DWH/logs/LogFactQueuePoliclinic.txt","w")
t0 = time.time()

#bikin koneksi ke db
conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)
conn_dwh_sqlserver = db_connection.create_connection(db_connection.dwh_sqlserver)

source = pd.read_sql_query("""
                           SELECT 
                                a.QueuePoliclinicID as QueuePoliclinicID,
                                a.AppointmentID as AppointmentID,
                                a.ActID as ActID,
                                a.PatientID as PatientID,
                                b.PatientSurrogateKey as PatientSurrogateKeyID,
                                a.AdmissionID as AdmissionID,
                                a.Method as Method,
                                a.QueueNum as QueueNum,
                                a.QueueDate as QueueDate,
                                a.DestOrgID as DestOrgID,
                                c.OrganizationSurrogateKey as DestOrgSurrogateKeyID,
                                a.ParentOrgID as ParentOrgID,
                                a.StatusCode as StatusCode,
                                a.CreatedDate as CreatedDate,
                                a.CreatedBy as CreatedBy,
                                a.CallDate as CallDate,
                                a.CallBy as CallBy,
                                a.CallTotal as CallTotal,
                                a.NurseStartDate as NurseStartDate,
                                a.NurseStopDate as NurseStopDate,
                                a.NurseIDStart as NurseIDStart,
                                a.NurseIDStop as NurseIDStop,
                                a.DocIDStart as DocIDStart,
                                a.DocIDStop as DocIDStop,
                                a.StatusCodeAnalis as StatusCodeAnalis,
                                a.CallTotalAnalis as CallTotalAnalis,
                                a.CallDateAnalis as CallDateAnalis,
                                a.AnalisID as AnalisID,
                                a.AnalisIDStart as AnalisIDStart,
                                a.AnalisIDStop as AnalisIDStop,
                                a.AnalisStartDate as AnalisStartDate,
                                a.AnalisStopDate as AnalisStopDate,
                                a.UpdateDate as UpdateDate,
                                a.CancelledBy as CancelledBy,
                                a.CancelledDate as CancelledDate
                            FROM staging_rscm.TransQueuePoliclinic a
                            LEFT JOIN staging_rscm.DimensionPatientMPI b on a.PatientID = b.PatientID and b.ScdActive = 1
                            LEFT JOIN staging_rscm.DimensionOrganization c on a.DestOrgID = c.ChildOrganizationID AND c.SCDActive = 1
                            WHERE 
                            -- CAST(a.QueueDate as date) = '2023-06-11'
                            CAST(InsertDateStaging as date) >= CAST(DATEADD(DAY, -1, GETDATE()) as date) AND CAST(InsertDateStaging as date) <= CAST(GETDATE() as date) 
                            OR CAST(UpdateDate as date) >= CAST(DATEADD(DAY, -1, GETDATE()) as date) AND CAST(UpdateDateStaging as date) <= CAST(GETDATE() as date)
                            AND b.MedicalNo NOT IN (SELECT MedicalNo FROM staging_rscm.DimensionDummyPatient)
                         """,con=conn_staging_sqlserver)
print(source)
if source.empty:
    print('tidak ada data dari source')
else:
    # jika dari source cuma 1 row
    if len(source) == 1:        
        # ambil primary key dari source, ambil index ke 0
        queuepoliclinicid = source["QueuePoliclinicID"].values[0]
        appointmentid = source["AppointmentID"].values[0]
        actid = source["ActID"].values[0]
        patientid = source["PatientID"].values[0]
        admissionid = source["AdmissionID"].values[0]
        destorgid = source["DestOrgID"].values[0]

        # query buat narik data dari target lalu filter berdasarkan primary key
        query = f'SELECT QueuePoliclinicID,AppointmentID,ActID,PatientID,PatientSurrogateKeyID, AdmissionID, Method,QueueNum,QueueDate,DestOrgID,DestOrgSurrogateKeyID,DestOrgParentOrgID,StatusCode,CreatedDate, CreatedBy,CallDate,CallBy,CallTotal,NurseStartDate,NurseStopDate,NurseIDStart,NurseIDStop,DocIDStart,DocIDStop,StatusCodeAnalis,CallTotalAnalis,CallDateAnalis,AnalisID,AnalisIDStart,AnalisIDStop,AnalisStartDate,AnalisStopDate,UpdateDate,CancelledBy,CancelledDate from dwhrscm_talend.FactQueuePoliclinic where QueuePoliclinicID IN ({queuepoliclinicid}) AND AppointmentID IN({appointmentid}) AND ActID IN ({actid}) AND PatientID IN ({patientid}) AND AdmissionID IN ({admissionid}) AND DestOrgID IN ({destorgid}) order by QueuePoliclinicID'
        target = pd.read_sql_query(query, conn_dwh_sqlserver)
    else :
         # ambil primary key dari source, pake unique biar tidak duplicate
        queuepoliclinicid = tuple(source["QueuePoliclinicID"].unique())
        appointmentid = tuple(source["AppointmentID"])
        actid = tuple(source["ActID"].unique())
        patientid = tuple(source["PatientID"].unique())
        admissionid = tuple(source["AdmissionID"].unique())
        destorgid = tuple(source["DestOrgID"])

         # query buat narik data dari target lalu filter berdasarkan primary key
        query = f'SELECT QueuePoliclinicID,AppointmentID,ActID,PatientID, PatientSurrogateKeyID,AdmissionID, Method,QueueNum,QueueDate,DestOrgID,DestOrgSurrogateKeyID,ParentOrgID,StatusCode,CreatedDate, CreatedBy,CallDate,CallBy,CallTotal,NurseStartDate,NurseStopDate,NurseIDStart,NurseIDStop,DocIDStart,DocIDStop,StatusCodeAnalis,CallTotalAnalis,CallDateAnalis,AnalisID,AnalisIDStart,AnalisIDStop,AnalisStartDate,AnalisStopDate,UpdateDate,CancelledBy,CancelledDate from dwhrscm_talend.FactQueuePoliclinic where QueuePoliclinicID IN {queuepoliclinicid} AND AppointmentID IN {appointmentid} AND ActID IN {actid} AND PatientID IN {patientid} AND AdmissionID IN {admissionid} AND DestOrgID IN {destorgid} order by QueuePoliclinicID'
        target = pd.read_sql_query(query, conn_dwh_sqlserver)

    # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

    # ambil data yang update dari changes
    modified = changes[changes[['QueuePoliclinicID','AppointmentID','ActID','PatientID','AdmissionID','DestOrgID']].apply(tuple,1).isin(target[['QueuePoliclinicID','AppointmentID','ActID','PatientID','AdmissionID','DestOrgID']].apply(tuple,1))]
    total_row_upd = len(modified)
    text_upd = f'total row update : {total_row_upd}'
    print(text_upd)
    print(modified)

    # ambil data yang new dari changes
    inserted = changes[~changes[['QueuePoliclinicID','AppointmentID','ActID','PatientID','AdmissionID','DestOrgID']].apply(tuple,1).isin(target[['QueuePoliclinicID','AppointmentID','ActID','PatientID','AdmissionID','DestOrgID']].apply(tuple,1))]
    total_row_ins = len(inserted)
    text_ins = f'total row inserted : {total_row_ins}'
    print(text_ins)
    print(inserted)

    if modified.empty:
        # bikin tanggal sekarang buat kolom InsertedDateDWH
        today = dt.datetime.now()
        today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
        inserted['InsertedDateDWH'] = today_convert
        inserted.to_sql('FactQueuePoliclinic', schema='dwhrscm_talend', con = conn_dwh_sqlserver, if_exists = 'append', index=False)
        print('success insert all data without update')
    
    else:
        #buat fungsi untuk update data ke tabel target
        def updated_to_sql(df, table_name, key_1,key_2,key_3,key_4,key_5,key_6):
            list_col = []
            table=table_name
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
                list_col.append(f'r.{col} = t.{col}')
            df.to_sql(temp_table,schema = 'dwhrscm_talend',con=conn_dwh_sqlserver, if_exists='replace',index=False)
            update_stmt_1 = f'UPDATE r '
            update_stmt_2 = f'SET '
            update_stmt_3 = ", ".join(list_col)
            update_stmt_8 = f' , r.UpdateDateDWH = CONVERT(DATETIME2(0), GETDATE(),120)'
            update_stmt_4 = f' FROM dwhrscm_talend.{table} r '
            update_stmt_5 = f'INNER JOIN (SELECT * FROM dwhrscm_talend.{temp_table}) as t ON r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} AND r.{pk_3} = t.{pk_3} AND r.{pk_4} = t.{pk_4} AND r.{pk_5} = t.{pk_5} AND r.{pk_6} = t.{pk_6} '
            update_stmt_6 = f'WHERE r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} AND r.{pk_3} = t.{pk_3} AND r.{pk_4} = t.{pk_4} AND r.{pk_5} = t.{pk_5} AND r.{pk_6} = t.{pk_6} '
            update_stmt_7 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_8 + update_stmt_4 + update_stmt_5 + update_stmt_6 +";"
            delete_stmt_1 = f'DROP TABLE dwhrscm_talend.{temp_table}'
            print(update_stmt_7)
            print(delete_stmt_1)
            conn_dwh_sqlserver.execute(update_stmt_7)
            conn_dwh_sqlserver.execute(delete_stmt_1)

        try:
            # update data
            updated_to_sql(modified, 'FactQueuePoliclinic', 'QueuePoliclinicID','AppointmentID', 'ActID','PatientID','AdmissionID','DestOrgID')

            # insert data baru
            today = dt.datetime.now()
            today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
            inserted['InsertedDateDWH'] = today_convert
            inserted.to_sql('FactQueuePoliclinic', schema='dwhrscm_talend', con=conn_dwh_sqlserver, if_exists ='append',index=False)
            print('success update and insert all data')
        
        except Exception as e:
            print(e)

#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)

db_connection.close_connection(conn_dwh_sqlserver)
db_connection.close_connection(conn_staging_sqlserver)