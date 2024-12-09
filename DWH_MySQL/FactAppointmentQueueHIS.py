from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine as ce
import pyodbc
import pandas as pd
import numpy as np
import time
import datetime as dt
date = dt.datetime.today()
import sys

sys.stdout = open("C:/TestPython/DWH_MySQL/logs/LogFactAppointmentQueueHISMySQL.txt","w")
t0 = time.time()

ehr = ce("mysql://hanif-ppi:hanif2022@172.16.19.11/ehr")
his =  ce('mysql://hanif-ppi:hanif2022@172.16.19.21/his')
dwh_mysql = ce("mysql://hanif-ppi:hanif2022@172.16.5.33/dwhrscm_talend")
dwh_sqlserver = ce('mssql+pyodbc://dev-ppi:D3vpp122!@172.16.19.36/DWH_RSCM?driver=SQL+Server+Native+Client+11.0', fast_executemany=True)

try :
    conn_ehr = ehr.connect()
    conn_his = his.connect()
    conn_dwh_mysql = dwh_mysql.connect()
    conn_dwh_sqlserver = dwh_sqlserver.connect()
    print('successfully connect to all database')
except SQLAlchemyError as e:
    error=str(e.__dict__['orig'])
    print(error)

source = pd.read_sql_query(""" SELECT 
                                    CASE 
                                        WHEN d.appointment_id IS NULL THEN 0
                                        ELSE d.appointment_id
                                    END AS AppointmentID,
                                    CONCAT(a.patient_id,'-',a.admission_id) AS KodeBooking,
                                    CASE
                                        WHEN a.payplan_id = 71 THEN 'JKN'
                                        ELSE 'Non JKN'
                                    END AS PatientType,
                                    a.patient_id AS PatientID,
                                    a.admission_id AS AdmissionID,
                                    a.admission_dttm AS AdmissionDate,
                                    b.patient_mrn_txt AS MedicalNo,
                                    a.payplan_no AS CardNo,
                                    -- SEPNo belum, join ke tabel EHR dulu
                                    c.ext_id AS NIK,
                                    d.phone_number as PhoneNo,
                                    d.poli_org_id as OrgID,
                                    -- Poli Code Belum mapping dari bpjs
                                    -- Poli Name Belum mapping dari bpjs
                                    '0' AS NewPatient,
                                    DATE(d.appointment_dttm) as AppointmentDate,
                                    d.dr_employee_id DoctorEmployeeID, -- buat join ke tabel mapping doctor
                                    TIME(d.appointment_dttm) as AppointmentTime,
                                    '1' AS VisitType,
                                    d.method AS AppointmentMethod,
                                    d.no_rujukan as ReferenceNo,
                                    d.queue_num as QueueNumber,
                                    d.queue_num as QueueNo,
                                    NULL as ServedEstimated,
                                    NULL as RestQuotaJKN,
                                    NULL as QuotaJKN,
                                    NULL as RestQuotaNonJKN,
                                    NULL as QuotaNonJKN,
                                    NULL as Notes
                                FROM xocp_his_patient_admission a 
                                LEFT JOIN xocp_his_patient b on a.patient_id = b.patient_id
                                LEFT JOIN xocp_persons c on b.person_id = c.person_id
                                LEFT JOIN xocp_his_patient_appointment d on CAST(d.patient_ext_id as int) = b.patient_mrn AND a.org_id = d.poli_org_id AND DATE(a.admission_dttm) = DATE(d.appointment_dttm)
                                WHERE 
                                -- (a.admission_dttm >= '2023-05-16 00:00:00' AND a.admission_dttm <= '2023-05-16 23:59:59')
                                (a.admission_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 5 DAY), "%%Y-%%m-%%d 00:00:00") 
                                AND a.admission_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 23:59:59"))
                                AND b.patient_mrn_txt NOT IN ('','10-00','10-01','10-02','10-03','10-04','10-05','10-06','10-07','10-08','10-09','10-10', '410-40-67','345-01-01','igd304-11-22','igd304-12-12','igd345-10-10','IGD349-11-22','IGD11-22-44','igd654-32-21','igd-01-19','igd89-10-11','351-10-11','367-50-43','lat-ih-an','9-79-99','19-02-13','380-54-91',
                                '380-54-92','380-54-93','380-54-94','380-54-95','380-54-96','380-54-97','380-54-98','380-55-00','380-55-01','380-55-02','380-55-03','LAT-JT-01','380-55-05','380-55-06','380-55-07','380-55-08','380-55-09','380-55-10','380-55-11','380-55-12','380-55-13','380-55-04','380-55-58','380-55-65','latihanp-ri-nt','200000-00-00','a1','a2',
                                'a3','393-82-05','400-60-38','400-62-73','400-62-74','400-70-44','400-70-83','400-70-99','378-63-59','378-63-61','378-63-62','378-63-63','378-63-64','408-78-44','407-44-56','13-13-13','14-14-14','421-94-60','407-51-98','407-52-02','36800','44470','44471','44472','44473','44474','44475','44476','44478','44479','410-40-67','455-37-54','455-37-56','455-37-60','455-37-63','455-37-66','455-37-67','455-37-68','410-40-67'
                                ,'10-10', '410-40-67','10-11','10-12','10-13','10-14', '10-15','10-16','10-17','10-18','10-19','10-20','10-21','10-22','10-23','10-24','10-25','10-26','10-27','10-28','10-29','10-30','10-31','10-32','10-33','10-34','10-35','10-36','10-37','10-38','10-39','10-40','10-41','10-42','10-43','10-44','10-45','10-46','10-47','10-48','10-49','10-50')
                                AND a.org_id IN (select org_id from xocp_orgs where parent_id in ('687','1872','2418') OR org_id in ('687','1872','2418'))
                                AND a.status_cd NOT IN ('nullified','cancelled')
                                ORDER BY AppointmentID """, con=conn_his)

source['Flag'] = '2'

# convert timedelta64[ns] ke time 
source['AppointmentTime'] = pd.to_datetime(source.AppointmentTime, unit='ns').dt.time
source['OrgID'] = source['OrgID'].replace(np.nan,0)
source['OrgID']=source['OrgID'].astype(int)


print(source)
source.replace({pd.NaT: None},inplace=True)
source.replace({np.nan: None},inplace=True)
source['QueueNumber'] = source['QueueNumber'].replace(np.nan,0)
source['QueueNumber']=source['QueueNumber'].astype(int)
source['QueueNo'] = source['QueueNo'].replace(np.nan,0)
source['QueueNo']=source['QueueNo'].astype(int)
source['DoctorEmployeeID'] = source['DoctorEmployeeID'].replace(np.nan,0)
source['DoctorEmployeeID']=source['DoctorEmployeeID'].astype(int)


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
        query = f"SELECT AppointmentID,KodeBooking,AdmissionDate,PatientID,AdmissionID,PatientType,MedicalNo,CardNo,SEPNo,NIK,PhoneNo,PoliCode,PoliName, NewPatient,AppointmentDate,DoctorCode,DoctorName,AppointmentTime,VisitType,AppointmentMethod,ReferenceNo,QueueNumber,QueueNo,ServedEstimated,RestQuotaJKN,QuotaJKN,RestQuotaNonJKN,QuotaNonJKN,Notes,Flag FROM FactAppointmentQueue where AppointmentID IN ({appointmentid}) AND PatientID IN ({patientid}) AND AdmissionID IN ({admissionid}) and Flag='2' order by AppointmentID"
        target = pd.read_sql_query(query, conn_dwh_mysql)
    else :
         # ambil primary key dari source, pake unique biar tidak duplicate
        appointmentid = tuple(source["AppointmentID"])
        patientid = tuple(source["PatientID"].unique())
        admissionid = tuple(source["AdmissionID"].unique())

         # query buat narik data dari target lalu filter berdasarkan primary key
        query = f"SELECT AppointmentID, KodeBooking,AdmissionDate,PatientID,AdmissionID,PatientType,MedicalNo,CardNo,SEPNo,NIK,PhoneNo,PoliCode,PoliName, NewPatient,AppointmentDate, DoctorCode,DoctorName,AppointmentTime,VisitType,AppointmentMethod,ReferenceNo,QueueNumber,QueueNo,ServedEstimated,RestQuotaJKN,QuotaJKN,RestQuotaNonJKN,QuotaNonJKN,Notes,Flag FROM FactAppointmentQueue where AppointmentID IN {appointmentid} AND PatientID IN {patientid} AND AdmissionID IN {admissionid} and Flag ='2' order by AppointmentID"
        target = pd.read_sql_query(query, conn_dwh_mysql)

    # convert timestamp ke format time, sama yang kaya source
    target['AppointmentTime'] = pd.to_datetime(target.AppointmentTime, unit='ns').dt.time    
    
    # replace value yang Nan jadi None
    target.replace({np.nan: None},inplace=True)
    target['QueueNumber'] = target['QueueNumber'].replace(np.nan,0)
    target['QueueNumber']=target['QueueNumber'].astype(int)
    target['QueueNo'] = target['QueueNo'].replace(np.nan,0)
    target['QueueNo']=target['QueueNo'].astype(int)

    query_get_sep = f"SELECT patient_id as PatientID,admission_id as AdmissionID,payplan_attr2 as SEPNo FROM xocp_ehr_patient_admission WHERE patient_id IN {patientid} and admission_id in {admissionid}"
    ehr_get_sep = pd.read_sql_query(query_get_sep, con=conn_ehr)
    print(ehr_get_sep.dtypes)

    query_get_doctorcode = f"SELECT doctorbpjs_id as DoctorCode, employee_id as DoctorEmployeeID FROM xocp_ehr_mapping_employee_doctorbpjs"
    ehr_get_doctorcode = pd.read_sql_query(query_get_doctorcode, con=conn_ehr)
    print(ehr_get_doctorcode.dtypes)

    query_get_doctorname = f"SELECT employee_id as DoctorEmployeeID,person_nm as DoctorName FROM xocp_hrm_employee"
    ehr_get_doctorname = pd.read_sql_query(query_get_doctorname, con=conn_ehr)
    print(ehr_get_doctorname.dtypes)

    query_get_policode = f"SELECT poli_cd as PoliCode, rs_poli_cd as OrgID, poli_nm as PoliName from xocp_maping_poli_bpjs_bc2 GROUP BY rs_poli_cd"
    ehr_get_policode = pd.read_sql_query(query_get_policode, con=conn_ehr)
    ehr_get_policode['OrgID']=ehr_get_policode['OrgID'].astype(int)
    print(ehr_get_policode.dtypes)

    source=source.merge(ehr_get_sep,how='left',on=['PatientID','AdmissionID']).merge(ehr_get_doctorcode,how='left',on='DoctorEmployeeID').merge(ehr_get_doctorname,how='left',on='DoctorEmployeeID').merge(ehr_get_policode,how='left',on='OrgID')
    
    new_order_columns =['AppointmentID','KodeBooking','AdmissionDate','PatientID','AdmissionID','PatientType','MedicalNo',
                        'CardNo','SEPNo','NIK','PhoneNo','PoliCode','PoliName','NewPatient','AppointmentDate','DoctorCode',
                        'DoctorName','AppointmentTime','VisitType','AppointmentMethod','ReferenceNo','QueueNumber','QueueNo',
                        'ServedEstimated','RestQuotaJKN','QuotaJKN','RestQuotaNonJKN','QuotaNonJKN','Notes','Flag']
    
    source = source.reindex(columns=new_order_columns)
    source.replace({np.nan: None},inplace=True)
    source['AppointmentID']=source['AppointmentID'].astype('int64')

    # cek tipe data source
    print('ini source yang baru')
    print(source.dtypes)
    
    # cek tipe data target
    print(target.dtypes)

    # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]
    # # print(source.apply(tuple,1).isin(target.apply(tuple,1)))
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
        # inserted['InsertDateStaging'] = today_convert
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
            # inserted['InsertDateStaging'] = today_convert
            inserted.to_sql('FactAppointmentQueue', con=conn_dwh_mysql, if_exists ='append',index=False)
            print('success update and insert all data')
        
        except Exception as e:
            print(e)

#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)

text=f'scheduler tanggal : {date}'
print(text)

conn_ehr.close()
conn_his.close()
conn_dwh_mysql.close()