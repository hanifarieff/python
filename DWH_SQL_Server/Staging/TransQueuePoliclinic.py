import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pyodbc
import pandas as pd
import time
import datetime as dt
import sys

sys.stdout = open("C:/TestPython/DWH_SQL_Server/Staging/logs/LogTransQueuePoliclinic.txt","w")
t0 = time.time()

# bikin koneksi ke db
conn_ehr = db_connection.create_connection(db_connection.replika_ehr)
conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)

source = pd.read_sql_query("""
                            SELECT
                                queue_policlinic_id as QueuePoliclinicID,
                                appointment_id as AppointmentID,
                                act_id as ActID,
                                patient_id as PatientID,
                                admission_id as AdmissionID,
                                method as Method,
                                queue_num as QueueNum,
                                queue_dttm as QueueDate,
                                dest_org_id as DestOrgID,
                                parent_org_id as ParentOrgID,
                                status_cd as StatusCode,
                                created_dttm as CreatedDate,
                                created_by as CreatedBy,
                                call_dttm as CallDate,
                                call_by as CallBy,
                                call_total as CallTotal,
                                nurse_start_dttm as NurseStartDate,
                                nurse_stop_dttm as NurseStopDate,
                                nurse_id_start as NurseIDStart,
                                nurse_id_stop as NurseIDStop,
                                doc_id_start as DocIDStart,
                                doc_id_stop as DocIDStop,
                                status_cd_analis as StatusCodeAnalis,
                                call_total_analis as CallTotalAnalis,
                                call_dttm_analis as CallDateAnalis,
                                analis_id as AnalisID,
                                analis_id_start as AnalisIDStart,
                                analis_id_stop as AnalisIDStop,
                                analis_start_dttm as AnalisStartDate,
                                analis_stop_dttm as AnalisStopDate,
                                updated_dttm as UpdateDate,
                                cancelled_by as CancelledBy,
                                cancelled_dttm as CancelledDate
                                FROM xocp_ehr_queue_policlinic
                                WHERE 
                                -- queue_dttm >= '2023-12-08 00:00:00' AND queue_dttm <= '2023-12-09 23:59:59'
                                (queue_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 00:00:00") 
		                        AND queue_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 23:59:59")) OR
                                (updated_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 00:00:00")
                                AND updated_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 23:59:59"))

                         """,con=conn_ehr)
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
        query = f'SELECT QueuePoliclinicID,AppointmentID,ActID,PatientID, AdmissionID, Method,QueueNum,QueueDate,DestOrgID,ParentOrgID,StatusCode,CreatedDate, CreatedBy,CallDate,CallBy,CallTotal,NurseStartDate,NurseStopDate,NurseIDStart,NurseIDStop,DocIDStart,DocIDStop,StatusCodeAnalis,CallTotalAnalis,CallDateAnalis,AnalisID,AnalisIDStart,AnalisIDStop,AnalisStartDate,AnalisStopDate,UpdateDate,CancelledBy,CancelledDate from staging_rscm.TransQueuePoliclinic where QueuePoliclinicID IN ({queuepoliclinicid}) AND AppointmentID IN({appointmentid}) AND ActID IN ({actid}) AND PatientID IN ({patientid}) AND AdmissionID IN ({admissionid}) AND DestOrgID IN ({destorgid}) order by QueuePoliclinicID'
        target = pd.read_sql_query(query, conn_staging_sqlserver)
    else :
         # ambil primary key dari source, pake unique biar tidak duplicate
        queuepoliclinicid = tuple(source["QueuePoliclinicID"].unique())
        appointmentid = tuple(source["AppointmentID"])
        actid = tuple(source["ActID"].unique())
        patientid = tuple(source["PatientID"].unique())
        admissionid = tuple(source["AdmissionID"].unique())
        destorgid = tuple(source["DestOrgID"])

         # query buat narik data dari target lalu filter berdasarkan primary key
        query = f'SELECT QueuePoliclinicID,AppointmentID,ActID,PatientID, AdmissionID, Method,QueueNum,QueueDate,DestOrgID,ParentOrgID,StatusCode,CreatedDate, CreatedBy,CallDate,CallBy,CallTotal,NurseStartDate,NurseStopDate,NurseIDStart,NurseIDStop,DocIDStart,DocIDStop,StatusCodeAnalis,CallTotalAnalis,CallDateAnalis,AnalisID,AnalisIDStart,AnalisIDStop,AnalisStartDate,AnalisStopDate,UpdateDate,CancelledBy,CancelledDate from staging_rscm.TransQueuePoliclinic where QueuePoliclinicID IN {queuepoliclinicid} AND AppointmentID IN {appointmentid} AND ActID IN {actid} AND PatientID IN {patientid} AND AdmissionID IN {admissionid} AND DestOrgID IN {destorgid} order by QueuePoliclinicID'
        target = pd.read_sql_query(query, conn_staging_sqlserver)

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
        # bikin tanggal sekarang buat kolom InsertDateStaging
        today = dt.datetime.now()
        today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
        inserted['InsertDateStaging'] = today_convert
        inserted.to_sql('TransQueuePoliclinic', schema='staging_rscm', con = conn_staging_sqlserver, if_exists = 'append', index=False)
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
            df.to_sql(temp_table,schema = 'staging_rscm',con=conn_staging_sqlserver, if_exists='replace',index=False)
            update_stmt_1 = f'UPDATE r '
            update_stmt_2 = f'SET '
            update_stmt_3 = ", ".join(list_col)
            update_stmt_8 = f' , r.UpdateDateStaging = CONVERT(DATETIME2(0), GETDATE(),120)'
            update_stmt_4 = f' FROM staging_rscm.{table} r '
            update_stmt_5 = f'INNER JOIN (SELECT * FROM staging_rscm.{temp_table}) as t ON r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} AND r.{pk_3} = t.{pk_3} AND r.{pk_4} = t.{pk_4} AND r.{pk_5} = t.{pk_5} AND r.{pk_6} = t.{pk_6} '
            update_stmt_6 = f'WHERE r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} AND r.{pk_3} = t.{pk_3} AND r.{pk_4} = t.{pk_4} AND r.{pk_5} = t.{pk_5} AND r.{pk_6} = t.{pk_6} '
            update_stmt_7 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_8 + update_stmt_4 + update_stmt_5 + update_stmt_6 +";"
            delete_stmt_1 = f'DROP TABLE staging_rscm.{temp_table}'
            print(update_stmt_7)
            print(delete_stmt_1)
            conn_staging_sqlserver.execute(update_stmt_7)
            conn_staging_sqlserver.execute(delete_stmt_1)

        try:
            # update data
            updated_to_sql(modified, 'TransQueuePoliclinic', 'QueuePoliclinicID','AppointmentID', 'ActID','PatientID','AdmissionID','DestOrgID')

            # insert data baru
            today = dt.datetime.now()
            today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
            inserted['InsertDateStaging'] = today_convert
            inserted.to_sql('TransQueuePoliclinic', schema='staging_rscm', con=conn_staging_sqlserver, if_exists ='append',index=False)
            print('success update and insert all data')
        
        except Exception as e:
            print(e)

#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)

date = dt.datetime.today()
text=f'scheduler tanggal : {date}'
print(text)

db_connection.close_connection(conn_ehr)
db_connection.close_connection(conn_staging_sqlserver)