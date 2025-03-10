from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine as ce
import pyodbc
import pandas as pd
import numpy as np
import time
import datetime as dt
import sys

sys.stdout = open("C:/TestPython/DWH_MySQL/logs/LogFactAppointmentQueueEHRMySQL.txt","w")
t0 = time.time()

ehr = ce("mysql://hanif-ppi:hanif2022@172.16.19.11/ehr")
dwh_mysql = ce("mysql://hanif-ppi:hanif2022@172.16.5.33/dwhrscm_talend")
# staging_sqlserver = ce('mssql+pyodbc://dev-ppi:D3vpp122!@172.16.19.36/StagingRSCM?driver=SQL+Server+Native+Client+11.0', fast_executemany=True) 
try :
    conn_ehr = ehr.connect()
    conn_dwh_mysql = dwh_mysql.connect()
    # conn_staging_sqlserver = staging_sqlserver.connect()
    print('successfully connect to all database')
except SQLAlchemyError as e:
    error=str(e.__dict__['orig'])
    print(error)

source = pd.read_sql_query(""" SELECT * FROM
                                (SELECT 
                                    CASE 
                                        WHEN d.appointment_id IS NULL THEN 0
                                        ELSE d.appointment_id
                                    END AS AppointmentID,
                                    CONCAT(a.patient_id,'-',a.admission_id) AS KodeBooking,
                                -- 	MAX(a.admission_dttm) as AdmissionDate,
                                -- 	MAX(c.admission_dttm) as MaxAdmissionDate,
                                    a.admission_dttm as AdmissionDate,
                                    a.patient_id as PatientID,
                                    a.admission_id as AdmissionID,
                                    CASE 
                                        WHEN a.payplan_id = 71 THEN 'JKN'
                                        ELSE 'Non JKN'
                                    END AS PatientType,
                                    b.patient_ext_id as MedicalNo,
                                    a.payplan_attr AS CardNo,
                                    a.payplan_attr2 as SEPNo,
                                    c.ext_id as NIK,
                                    CASE 
                                        WHEN SUBSTRING_INDEX(SUBSTRING_INDEX(c.telecom, '|', -2), '|', 1) = '' THEN
                                        SUBSTRING_INDEX(c.telecom, '|', 1)
                                        ELSE SUBSTRING_INDEX(SUBSTRING_INDEX(c.telecom, '|', -2), '|', 1)
                                    END AS PhoneNo,
                                    h.poli_cd as PoliCode,
                                    h.poli_nm as PoliName,
                                    '0' as NewPatient,
                                    DATE(d.appointment_dttm) as AppointmentDate,
                                    e.doctorbpjs_id AS DoctorCode,
                                    f.person_nm AS DoctorName,
                                    TIME(d.appointment_dttm) as AppointmentTime,
                                    '1' as VisitType,
                                    d.method AS AppointmentMethod,
                                    d.no_rujukan AS ReferenceNo,
                                    d.antrian AS QueueNumber,
                                    d.antrian as QueueNo,
                                    NULL as ServedEstimated,
                                    NULL as RestQuotaJKN,
                                    NULL as QuotaJKN,
                                    NULL as RestQuotaNonJKN,
                                    NULL as QuotaNonJKN,
                                    NULL as Notes,
                                    DENSE_RANK() OVER(PARTITION BY a.patient_id,a.admission_id ORDER BY a.admission_id DESC) Rank
                                FROM xocp_ehr_patient_admission AS a
                                LEFT JOIN xocp_ehr_patient AS b ON b.patient_id = a.patient_id
                                LEFT JOIN xocp_persons AS c ON c.person_id = b.person_id
                                LEFT JOIN xocp_ehr_appointment AS d ON d.mrn = b.patient_ext_id AND d.org_id = a.org_id AND DATE(d.appointment_dttm) = DATE(a.admission_dttm)
                                LEFT JOIN xocp_ehr_mapping_employee_doctorbpjs AS e ON d.employee_id = e.employee_id
                                LEFT JOIN xocp_hrm_employee AS f ON f.employee_id = d.employee_id
                                -- LEFT JOIN xocp_ehr_appointment_slot AS g ON d.appointment_slot_id = g.appointment_slot_id
                                LEFT JOIN (select rs_poli_cd,poli_cd,poli_nm from xocp_maping_poli_bpjs_bc2
                                GROUP BY rs_poli_cd) h ON h.rs_poli_cd = d.org_id
                                WHERE
                                -- (a.admission_dttm >= '2023-05-11 00:00:00' AND a.admission_dttm <= '2023-05-15 23:59:59') 
                                (a.admission_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 7 DAY), "%%Y-%%m-%%d 00:00:00") 
                                AND a.admission_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 23:59:59"))
                                AND a.status_cd NOT IN ('nullified','cancelled')
                                AND a.org_id in (select org_id from xocp_orgs where parent_id not in ('687','1872','2418') and org_id not in ('687','1872','2418'))
                                ORDER BY d.appointment_id) new
                                WHERE new.rank = 1 and new.MedicalNo NOT IN ('','10-00','10-01','10-02','10-03','10-04','10-05','10-06','10-07','10-08','10-09','10-10', '410-40-67',
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
                                -- AND new.KodeBooking IN ('762370-181','1663944-2') 
                                ORDER BY new.AppointmentID""", con=conn_ehr)

# ambil semua kolom kecuali kolom Rank
source = source.loc[:,source.columns != 'Rank']
source['Flag'] = '1'

# convert timedelta64[ns] ke time 
source['AppointmentTime'] = pd.to_datetime(source.AppointmentTime, unit='ns').dt.time

print(source)
source.replace({pd.NaT: None},inplace=True)
source.replace({np.nan: None},inplace=True)
source['QueueNumber'] = source['QueueNumber'].replace(np.nan,0)
source['QueueNumber']=source['QueueNumber'].astype(int)
source['QueueNo'] = source['QueueNo'].replace(np.nan,0)
source['QueueNo']=source['QueueNo'].astype(int)

# cek tipe data source
print(source.dtypes)

# try:
#     source.to_sql('FactAppointmentQueue',schema='staging_rscm',con=conn_staging_sqlserver, if_exists='append',index=False)
#     print('success inserted')
# except Exception as e:
#     print(e)

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
        query = f"SELECT AppointmentID,KodeBooking,AdmissionDate,PatientID,AdmissionID,PatientType,MedicalNo,CardNo,SEPNo,NIK,PhoneNo,PoliCode,PoliName, NewPatient,AppointmentDate,DoctorCode,DoctorName,AppointmentTime,VisitType,AppointmentMethod,ReferenceNo,QueueNumber,QueueNo,ServedEstimated,RestQuotaJKN,QuotaJKN,RestQuotaNonJKN,QuotaNonJKN,Notes,Flag FROM FactAppointmentQueue where AppointmentID IN ({appointmentid}) AND PatientID IN ({patientid}) AND AdmissionID IN ({admissionid}) and Flag='1' order by AppointmentID"
        target = pd.read_sql_query(query, conn_dwh_mysql)
    else :
         # ambil primary key dari source, pake unique biar tidak duplicate
        appointmentid = tuple(source["AppointmentID"].unique())
        patientid = tuple(source["PatientID"].unique())
        admissionid = tuple(source["AdmissionID"].unique())

         # query buat narik data dari target lalu filter berdasarkan primary key
        query = f"SELECT AppointmentID,KodeBooking,AdmissionDate,PatientID,AdmissionID,PatientType,MedicalNo,CardNo,SEPNo,NIK,PhoneNo,PoliCode,PoliName, NewPatient,AppointmentDate,DoctorCode,DoctorName,AppointmentTime,VisitType,AppointmentMethod,ReferenceNo,QueueNumber,QueueNo,ServedEstimated,RestQuotaJKN,QuotaJKN,RestQuotaNonJKN,QuotaNonJKN,Notes,Flag FROM FactAppointmentQueue where AppointmentID IN {appointmentid} AND PatientID IN {patientid} AND AdmissionID IN {admissionid} and Flag ='1' order by AppointmentID"
        target = pd.read_sql_query(query, conn_dwh_mysql)

    target['AppointmentTime'] = pd.to_datetime(target.AppointmentTime, unit='ns').dt.time
    # replace value yang Nan jadi None
    target.replace({np.nan: None},inplace=True)
    #target['AppointmentTime'] = pd.to_datetime(target.AppointmentTime, unit='ns').dt.time

    target['QueueNumber'] = target['QueueNumber'].replace(np.nan,0)
    target['QueueNumber']=target['QueueNumber'].astype(int)
    target['QueueNo'] = target['QueueNo'].replace(np.nan,0)
    target['QueueNo']=target['QueueNo'].astype(int)

    # cek tipe data target
    print(target.dtypes)

    # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]
    # print(source.apply(tuple,1).isin(target.apply(tuple,1)))
    # print(source.iloc[:,[17,19]])
    # print(target.iloc[:,[17,19]])
    # print(source.iloc[:,0:4].apply(tuple,1))
    # print(target.iloc[:,0:4].apply(tuple,1))
    # print(source.iloc[:,3:5].apply(tuple,1))
    # print(target.iloc[:,3:5].apply(tuple,1))
    # print(source.iloc[:,5:7].apply(tuple,1))
    # print(target.iloc[:,5:7].apply(tuple,1))
    # print(source.iloc[:,6:10].apply(tuple,1))
    # print(target.iloc[:,6:10].apply(tuple,1))
    # print(source.iloc[:,9:13].apply(tuple,1))
    # print(target.iloc[:,9:13].apply(tuple,1))
    # print(source.iloc[:,12:16].apply(tuple,1))
    # print(target.iloc[:,12:16].apply(tuple,1))
    # print(source.iloc[:,14:19].apply(tuple,1))
    # print(target.iloc[:,14:19].apply(tuple,1))
    # print(source.iloc[:,18:22].apply(tuple,1))
    # print(target.iloc[:,18:22].apply(tuple,1))
    # print(source.iloc[:,21:26].apply(tuple,1))
    # print(target.iloc[:,21:26].apply(tuple,1))
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
        # today = dt.datetime.now()
        # today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
        # inserted['InsertDateDWH'] = today_convert
        inserted.to_sql('FactAppointmentQueue', con = conn_dwh_mysql, if_exists = 'append', index=False)
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
            df.to_sql(temp_table,con=conn_dwh_mysql, if_exists='replace',index=False)
            update_stmt_1 = f'UPDATE {table} r '
            update_stmt_2 = f'INNER JOIN (SELECT * FROM {temp_table}) as t ON r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} AND r.{pk_3} = t.{pk_3} AND r.{pk_4} = t.{pk_4} '
            update_stmt_3 = f'SET '
            update_stmt_4 = ", ".join(list_col)
            update_stmt_5 = f' WHERE r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} AND r.{pk_3} = t.{pk_3} AND r.{pk_4} = t.{pk_4} '
            update_stmt_6 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_4 + update_stmt_5 +";"
            delete_stmt_1 = f'DROP TABLE {temp_table}'
            print(update_stmt_6)
            conn_dwh_mysql.execute(update_stmt_6)
            conn_dwh_mysql.execute(delete_stmt_1)

        try:
            # update data
            updated_to_sql(modified, 'FactAppointmentQueue', 'AppointmentID','PatientID','AdmissionID','Flag')

            # insert data baru
            # today = dt.datetime.now()
            # today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
            # inserted['InsertDateDWH'] = today_convert
            inserted.to_sql('FactAppointmentQueue', con=conn_dwh_mysql, if_exists ='append',index=False)
            print('success update and insert all data')
        
        except Exception as e:
            print(e)

#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)

conn_ehr.close()
conn_dwh_mysql.close()
# conn_staging_sqlserver.close()