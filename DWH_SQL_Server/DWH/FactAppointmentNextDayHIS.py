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
sys.stdout = open("C:/TestPython/DWH_SQL_Server/DWH/logs/LogFactAppointmentNextDayHIS.txt","w")
t0 = time.time()

# bikin koneksi ke db
conn_dwh_sqlserver = db_connection.create_connection(db_connection.dwh_sqlserver)
conn_dwh_mysql = db_connection.create_connection(db_connection.dwh_mysql)
conn_ehr = db_connection.create_connection(db_connection.replika_ehr)
conn_his = db_connection.create_connection(db_connection.replika_his)

source = pd.read_sql_query(""" 
                        SELECT 
                                CASE
                                        WHEN a.payplan_id = '71' THEN a.appointment_id_origin
                                        ELSE a.appointment_id 
                                END as AppointmentID,
                                '-' as KodeBooking,
                                NULL as AdmissionDate,
                                CASE
                                                WHEN a.patient_id IS NULL THEN 0
                                                ELSE a.patient_id
                                END as PatientID,
                                0 AS AdmissionID,
                                CASE
                                                WHEN b.patient_mrn_txt IS NULL THEN '-'
                                                ELSE b.patient_mrn_txt
                                END as MedicalNo,
                                CASE
                                                WHEN a.payplan_id = '71' THEN 'JKN'
                                                ELSE 'Non JKN'
                                END PatientType,
                 		CASE	
                 				WHEN a.card_number IS NULL OR a.card_number ='' THEN '-'
                 				ELSE a.card_number
                 		END as CardNo,
                                '-' as SEPNo,
                                CASE
                                                WHEN c.ext_id IS NULL THEN '-'
                                                ELSE c.ext_id
                                END AS NIK,
                                a.phone_number as PhoneNo,
                                a.poli_org_id as OrgID,
                                '0' as NewPatient,
                                CASE 
                                        WHEN a.appointment_id IS NULL THEN NULL
                                        ELSE a.dr_employee_id 
                                END AS DoctorEmployeeID, -- buat join ke tabel mapping doctor
                                CASE
                                        WHEN a.appointment_id IS NULL THEN NULL
                                        ELSE DATE(a.appointment_dttm) 
                                END AS AppointmentDate,
                                CASE
                                        WHEN a.appointment_id IS NULL THEN NULL
                                        ELSE TIME(a.appointment_dttm) 
                                END AS AppointmentTime,
                                a.status_cd as StatusAppointment,
                                CASE
                                        WHEN a.appointment_id IS NULL THEN TIME(DATE_FORMAT(DATE_ADD(a.appointment_dttm, INTERVAL 30 MINUTE),'%%Y-%%m-%%d %%H:00:00'))
                                        ELSE 
                                                CASE 
                                                        WHEN a.appointment_dttm IS NULL THEN TIME(DATE_FORMAT(DATE_ADD(a.appointment_dttm, INTERVAL 30 MINUTE),'%%Y-%%m-%%d %%H:00:00')) 
                                                        ELSE TIME(DATE_FORMAT(DATE_ADD(a.appointment_dttm, INTERVAL 30 MINUTE),'%%Y-%%m-%%d %%H:00:00'))
                                                END
                                END AS DoctorPracticeStartTime,
                                CASE
                                        WHEN a.appointment_id IS NULL THEN TIME(DATE_FORMAT(DATE_ADD(a.appointment_dttm, INTERVAL 2 HOUR),'%%Y-%%m-%%d %%H:00:00')) 
                                        ELSE
                                                CASE
                                                        WHEN a.appointment_dttm IS NULL THEN TIME(DATE_FORMAT(DATE_ADD(a.appointment_dttm, INTERVAL 2 HOUR),'%%Y-%%m-%%d %%H:00:00')) 
                                                        ELSE TIME(DATE_FORMAT(DATE_ADD(a.appointment_dttm, INTERVAL 2 HOUR),'%%Y-%%m-%%d %%H:00:00'))  
                                                END
                                END as DoctorPracticeFinishTime,
                                '2' AS VisitType,
                                CASE
                                        WHEN a.method IS NULL THEN '-'
                                        ELSE a.method
                                END AS AppointmentMethod,
                                CASE
                                        WHEN a.no_rujukan IS NULL THEN NULL
                                        ELSE a.no_rujukan
                                END AS ReferenceNo,
                                CASE 
                                        WHEN a.queue_num IS NULL THEN 5
                                        ELSE a.queue_num 
                                END AS QueueNumber,
                                CASE 
                                        WHEN a.queue_num IS NULL THEN 5
                                        ELSE a.queue_num 
                                END AS QueueNo,
                                CONCAT(DATE(a.appointment_dttm), ' ',TIME(a.appointment_dttm)) as ServedEstimated,
                                CASE 
                                        WHEN a.payplan_id = 71 THEN 5
                                        ELSE 0
                                END AS RestQuotaJKN,
                                CASE 
                                        WHEN a.payplan_id = 71 THEN 50 
                                        ELSE 0
                                END AS QuotaJKN,
                                0 as RestQuotaNonJKN,
                                0 as QuotaNonJKN,
                                '-' as Notes
                        FROM xocp_his_patient_appointment a 
                        LEFT JOIN xocp_his_patient b on a.patient_id = b.patient_id
                        LEFT JOIN xocp_persons c on b.person_id = c.person_id
                        WHERE 
                        a.appointment_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL -1 DAY), "%%Y-%%m-%%d 00:00:00")
                        AND a.appointment_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL -1 DAY), "%%Y-%%m-%%d 23:59:59")
                        -- a.appointment_dttm >= '2024-09-27 00:00:00' AND a.appointment_dttm <= '2024-09-27 23:59:59'
                        AND poli_org_id IN (select org_id from xocp_orgs where parent_id in ('687','1872','2418') OR org_id in ('687','1872','2418'))
                        and b.patient_mrn_txt NOT IN ('','10-00','10-01','10-02','10-03','10-04','10-05','10-06','10-07','10-08','10-09','10-10', '410-40-67',
                        '345-01-01','igd304-11-22','igd304-12-12','igd345-10-10','IGD349-11-22','IGD11-22-44','igd654-32-21','igd-01-19',
                        'igd89-10-11','351-10-11','367-50-43','lat-ih-an','9-79-99','19-02-13','380-54-91',
                        '380-54-92','380-54-93','380-54-94','380-54-95',
                        '380-54-96','380-54-97','380-54-98','380-55-00','380-55-01',
                        '380-55-02','380-55-03','LAT-JT-01','380-55-05','380-55-06',
                        '380-55-07','380-55-08','380-55-09','380-55-10','380-55-11','380-55-12','380-55-13','380-55-04',
                        '380-55-58','380-55-65','latihanp-ri-nt','200000-00-00','a1','a2',
                        'a3','393-82-05','400-60-38','400-62-73','400-62-74',
                        '400-70-44','400-70-83','400-70-99','378-63-59',
                        '378-63-61','378-63-62','378-63-63','378-63-64','408-78-44','407-44-56',
                        '13-13-13','14-14-14','421-94-60','407-51-98',
                        '407-52-02','36800','44470','44471','44472','44473',
                        '44474','44475','44476','44478','44479','410-40-67','455-37-54','455-37-56','455-37-60',
                        '455-37-63','455-37-66','455-37-67','455-37-68','410-40-67'
                        ,'10-10', '410-40-67','10-11','10-12','10-13','10-14', '10-15','10-16','10-17','10-18','10-19','10-20','10-21','10-22','10-23','10-24','10-25',
                        '10-26','10-27','10-28','10-29','10-30','10-31','10-32','10-33','10-34','10-35','10-36',
                        '10-37','10-38','10-39','10-40',
                        '10-41','10-42','10-43','10-44','10-45','10-46','10-47','10-48','10-49','10-50','465-40-63')
                        -- AND a.appointment_id IN (14946221)
                        AND a.status_cd NOT IN ('cancelled','nullified')
                        order by a.appointment_id
                                """, con=conn_his)

source['Flag'] = '2'
# convert timedelta64[ns] ke time 
source['AppointmentTime'] = pd.to_datetime(source.AppointmentTime, unit='ns').dt.time
source['DoctorPracticeStartTime'] = pd.to_datetime(source.DoctorPracticeStartTime, unit='ns').dt.time
source['DoctorPracticeFinishTime'] = pd.to_datetime(source.DoctorPracticeFinishTime, unit='ns').dt.time
source['OrgID'] = source['OrgID'].replace(np.nan,0)
source['OrgID']=source['OrgID'].astype(int)

print(source[source['AppointmentID']==14946221])
source.replace({pd.NaT: None},inplace=True)
source.replace({np.nan: None},inplace=True)
source['QueueNumber'] = source['QueueNumber'].replace(np.nan,0)
source['QueueNumber']=source['QueueNumber'].astype('int64')
source['QueueNo'] = source['QueueNo'].replace(np.nan,0)
source['QueueNo']=source['QueueNo'].astype('int64')
source['DoctorEmployeeID'] = source['DoctorEmployeeID'].replace(np.nan,0)
source['DoctorEmployeeID']=source['DoctorEmployeeID'].astype(int)

if source.empty:
    print('tidak ada data dari source')
else:
    # jika dari source cuma 1 row
    if len(source) == 1:        
        # ambil primary key dari source, ambil index ke 0
        appointmentid = source["AppointmentID"].values[0]
        patientid = source=["PatientID"].values[0]
        admissionid = source=["AdmissionID"].values[0]

        # query buat narik data dari target lalu filter berdasarkan primary key
        query = f"SELECT AppointmentID,TRIM(KodeBooking) as KodeBooking,AdmissionDate,PatientID,AdmissionID,MedicalNo,PatientType,CardNo,SEPNo,ReferenceNo,NIK,PhoneNo,PoliCode,PoliName, NewPatient,TRIM(DoctorCode) AS DoctorCode,DoctorName,AppointmentDate,AppointmentTime,StatusAppointment,DoctorPracticeStartTime,DoctorPracticeFinishTime,VisitType,AppointmentMethod,QueueNumber,QueueNo,ServedEstimated,RestQuotaJKN,QuotaJKN,RestQuotaNonJKN,QuotaNonJKN,Notes,Flag FROM dwhrscm_talend.FactAppointmentNextDay where AppointmentID IN ({appointmentid}) AND PatientID IN ({patientid}) AND AdmissionID IN ({admissionid}) and Flag='2' order by AppointmentID"
        target = pd.read_sql_query(query, conn_dwh_sqlserver)
    else :
         # ambil primary key dari source, pake unique biar tidak duplicate
        appointmentid = tuple(source["AppointmentID"])
        patientid = tuple(source["PatientID"].unique())
        admissionid = tuple(source["AdmissionID"])

         # query buat narik data dari target lalu filter berdasarkan primary key
        query = f"SELECT AppointmentID,TRIM(KodeBooking) as KodeBooking,AdmissionDate,PatientID,AdmissionID,MedicalNo,PatientType,CardNo,SEPNo,ReferenceNo,NIK,PhoneNo,PoliCode,PoliName, NewPatient, TRIM(DoctorCode) AS DoctorCode,DoctorName,AppointmentDate,AppointmentTime,StatusAppointment,DoctorPracticeStartTime,DoctorPracticeFinishTime,VisitType,AppointmentMethod,QueueNumber,QueueNo,ServedEstimated,RestQuotaJKN,QuotaJKN,RestQuotaNonJKN,QuotaNonJKN,Notes,Flag FROM dwhrscm_talend.FactAppointmentNextDay where AppointmentID IN {appointmentid} AND PatientID IN {patientid} AND AdmissionID IN {admissionid} and Flag ='2' order by AppointmentID"
        target = pd.read_sql_query(query, conn_dwh_sqlserver)
        
    # replace value yang Nan jadi None
    target.replace({np.nan: None},inplace=True)
    # target['QueueNumber'] = target['QueueNumber'].replace(np.nan,0)
    # target['QueueNumber']=target['QueueNumber'].astype(int)
    # target['QueueNo'] = target['QueueNo'].replace(np.nan,0)
    # target['QueueNo']=target['QueueNo'].astype(int)

    query_get_doctorcode = f"""SELECT x.employee_id as DoctorEmployeeID, x.doctorbpjs_id as DoctorCode
                                FROM
                                (select employee_id,doctorbpjs_id,DENSE_RANK()OVER(PARTITION BY employee_id ORDER BY updated_dttm DESC) ranked from xocp_ehr_mapping_employee_doctorbpjs
                                -- where employee_id = 5614
                                ) x
                                where x.ranked = 1"""
    ehr_get_doctorcode = pd.read_sql_query(query_get_doctorcode, con=conn_ehr)
    print(ehr_get_doctorcode.dtypes)

    query_get_doctorname = f"SELECT employee_id as DoctorEmployeeID,person_nm as DoctorName FROM xocp_hrm_employee"
    ehr_get_doctorname = pd.read_sql_query(query_get_doctorname, con=conn_ehr)
    print(ehr_get_doctorname.dtypes)

    query_get_policode = f"SELECT poli_cd as PoliCode, rs_poli_cd as OrgID, poli_nm as PoliName from xocp_maping_poli_bpjs_bc2 GROUP BY rs_poli_cd"
    ehr_get_policode = pd.read_sql_query(query_get_policode, con=conn_ehr)
    ehr_get_policode['OrgID']=ehr_get_policode['OrgID'].astype(int)
    print(ehr_get_policode.dtypes)

    source=source.merge(ehr_get_doctorcode,how='left',on='DoctorEmployeeID').merge(ehr_get_doctorname,how='left',on='DoctorEmployeeID').merge(ehr_get_policode,how='left',on='OrgID')

    new_order_columns =['AppointmentID','KodeBooking','AdmissionDate','PatientID','AdmissionID','MedicalNo','PatientType',
                        'CardNo','SEPNo','ReferenceNo','NIK','PhoneNo','PoliCode','PoliName','NewPatient','DoctorCode',
                        'DoctorName','AppointmentDate','AppointmentTime','StatusAppointment','DoctorPracticeStartTime','DoctorPracticeFinishTime',
                        'VisitType','AppointmentMethod','QueueNumber','QueueNo',
                        'ServedEstimated','RestQuotaJKN','QuotaJKN','RestQuotaNonJKN','QuotaNonJKN','Notes','Flag']
    
    source = source.reindex(columns=new_order_columns)
    print(source[source['AppointmentID']==14946221])
    print('ini empty string')
    # source['SEPNo'].fillna(source['SEPNoSpecial'],inplace=True)
    # semua yang null dari source diganti jadi strip '-' karenada SEP dan rujukan yg null
    # source.replace({np.nan: '-'},inplace=True)
    # print(source.iloc[:,7:10])

    # source = source.loc[:,source.columns != 'SEPNoSpecial']
    print('after join')
    source['PatientID'] = source['PatientID'].astype('int64')
    # source['CardNo'] = source['CardNo'].astype('str')
    source.replace({np.nan: None},inplace=True)
    target.replace({np.nan: None},inplace=True)
    print(source.dtypes)
    # cek tipe data target
    print(target.dtypes)

    # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

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

    # ambil data yang update dari changes
    modified = changes[changes[['AppointmentID','PatientID','AdmissionID','Flag']].apply(tuple,1).isin(target[['AppointmentID','PatientID','AdmissionID','Flag']].apply(tuple,1))]
    total_row_upd = len(modified)
    text_upd = f'total row update : {total_row_upd}'
    print(text_upd)
    print(modified.iloc[:,0:6])

    # ambil data yang new dari changes
    inserted = changes[~changes[['AppointmentID','PatientID','AdmissionID','Flag']].apply(tuple,1).isin(target[['AppointmentID','PatientID','AdmissionID','Flag']].apply(tuple,1))]
    total_row_ins = len(inserted)
    text_ins = f'total row inserted : {total_row_ins}'
    print(text_ins)
    print(inserted.iloc[:,0:6])
    
    if modified.empty:
        # bikin tanggal sekarang buat kolom InsertDateStaging
        today = dt.datetime.now()
        today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
        inserted['InsertedDateDWH'] = today_convert
        inserted.to_sql('FactAppointmentNextDay', schema='dwhrscm_talend', con = conn_dwh_sqlserver, if_exists = 'append', index=False)
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
            update_stmt_4 = f' FROM dwhrscm_talend.{table} r '
            update_stmt_5 = f'INNER JOIN (SELECT * FROM dwhrscm_talend.{temp_table}) as t ON r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} AND r.{pk_3} = t.{pk_3} AND r.{pk_4} = t.{pk_4} '
            update_stmt_6 = f'WHERE r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} AND r.{pk_3} = t.{pk_3} AND r.{pk_4} = t.{pk_4} '
            update_stmt_7 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_8 + update_stmt_4 + update_stmt_5 + update_stmt_6 +";"
            delete_stmt_1 = f'DROP TABLE dwhrscm_talend.{temp_table}'
            print(update_stmt_7)
            conn_dwh_sqlserver.execute(update_stmt_7)
            conn_dwh_sqlserver.execute(delete_stmt_1)

        try:
            # update data
            updated_to_sql(modified, 'FactAppointmentNextDay', 'AppointmentID','PatientID','AdmissionID','Flag')

            # insert data baru
            today = dt.datetime.now()
            today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
            inserted['InsertedDateDWH'] = today_convert
            inserted.to_sql('FactAppointmentNextDay', schema='dwhrscm_talend', con=conn_dwh_sqlserver, if_exists ='append',index=False)
            print('success update and insert all data')
        
        except Exception as e:
            print(e)

#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)

text=f'scheduler tanggal : {date}'
print(text)

db_connection.close_connection(conn_ehr)
db_connection.close_connection(conn_dwh_mysql)
db_connection.close_connection(conn_dwh_sqlserver)