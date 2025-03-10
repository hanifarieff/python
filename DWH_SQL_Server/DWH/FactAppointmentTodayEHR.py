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
sys.stdout = open("C:/TestPython/DWH_SQL_Server/DWH/logs/LogFactAppointmentTodayEHR.txt","w")
t0 = time.time()

# bikin koneksi ke db
conn_dwh_sqlserver = db_connection.create_connection(db_connection.dwh_sqlserver)
conn_dwh_mysql = db_connection.create_connection(db_connection.dwh_mysql)
conn_ehr = db_connection.create_connection(db_connection.replika_ehr)

source = pd.read_sql_query(""" SELECT
                                        a.appointment_id as AppointmentID,
                                        '-' as KodeBooking,
                                        NULL as AdmissionDate,
                                        CASE
                                            WHEN b.patient_id IS NULL THEN 0
                                            ELSE b.patient_id
                                        END as PatientID,
                                        0 AS AdmissionID,
                                        CASE
                                            WHEN a.mrn IS NULL THEN '-'
                                            ELSE a.mrn
                                        END as MedicalNo,
                                        CASE
                                            WHEN a.payplan_id = '71' THEN 'JKN'
                                            ELSE 'Non JKN'
                                        END PatientType,
                                        CASE	
                                            WHEN a.card_no IS NULL OR a.card_no ='' THEN '-'
                                            ELSE a.card_no
                                        END as CardNo,
                                        '-' as SEPNo,
                                        CASE
                                            WHEN a.no_rujukan IS NULL THEN NULL
                                            ELSE a.no_rujukan
                                        END AS ReferenceNo,
                                        CASE
                                            WHEN c.ext_id IS NULL THEN '-'
                                            ELSE c.ext_id
                                        END AS NIK,
                                        CASE 
                                            WHEN SUBSTRING_INDEX(SUBSTRING_INDEX(c.telecom, '|', -2), '|', 1) = '' THEN
                                            SUBSTRING_INDEX(c.telecom, '|', 1)
                                            WHEN SUBSTRING_INDEX(SUBSTRING_INDEX(c.telecom, '|', -2), '|', 1) IS NULL THEN '-'
                                            ELSE SUBSTRING_INDEX(SUBSTRING_INDEX(c.telecom, '|', -2), '|', 1)
                                        END AS PhoneNo,
                                        CASE
                                            WHEN d.poli_cd IS NULL THEN '-'
                                            ELSE d.poli_cd
                                        END AS PoliCode,
                                        CASE
                                            WHEN d.poli_nm IS NULL THEN '-'
                                            ELSE d.poli_nm
                                        END AS PoliName,
                                        '0' AS NewPatient,
                                        CASE
                                            WHEN TRIM(e.doctorbpjs_id) IS NULL THEN '-'
                                            ELSE TRIM(e.doctorbpjs_id)
                                        END AS DoctorCode,
                                        CASE
                                            WHEN f.person_nm IS NULL THEN '-'
                                            ELSE f.person_nm
                                        END AS DoctorName,
                                        DATE(a.appointment_dttm) as AppointmentDate,
                                        TIME(a.appointment_dttm) as AppointmentTime,
                                        a.status_cd as StatusAppointment,
                                        CASE 
                                            -- WHEN a.appointment_id IS NULL THEN TIME(a.admission_dttm)
                                            WHEN a.appointment_id IS NULL THEN TIME(DATE_FORMAT(DATE_ADD(a.appointment_dttm, INTERVAL 30 MINUTE),'%%Y-%%m-%%d %%H:00:00'))
                                            ELSE 
                                                CASE
                                                    WHEN a.method = 'langsung' THEN 
                                                        CASE
                                                            WHEN jad.start_work_dttm IS NULL THEN TIME(DATE_FORMAT(DATE_ADD(a.appointment_dttm, INTERVAL 30 MINUTE),'%%Y-%%m-%%d %%H:00:00'))
                                                            ELSE TIME(jad.start_work_dttm)
                                                        END
                                                    WHEN a.method = 'rscmku' THEN TIME(jad.start_work_dttm)
                                                    WHEN a.method = 'rawat_inap' THEN 
                                                        CASE
                                                            WHEN jad.start_work_dttm IS NULL THEN TIME(DATE_FORMAT(DATE_ADD(a.appointment_dttm, INTERVAL 30 MINUTE),'%%Y-%%m-%%d %%H:00:00'))
                                                            ELSE TIME(jad.start_work_dttm)
                                                        END
                                                    ELSE TIME(DATE_FORMAT(DATE_ADD(a.appointment_dttm, INTERVAL 30 MINUTE),'%%Y-%%m-%%d %%H:00:00'))
                                                END
                                        END AS DoctorPracticeStartTime,
                                        CASE 
                                            WHEN a.appointment_id IS NULL THEN TIME(DATE_FORMAT(DATE_ADD(a.appointment_dttm, INTERVAL 2 HOUR),'%%Y-%%m-%%d %%H:00:00'))
                                            ELSE 
                                                CASE
                                                    WHEN a.method = 'langsung' THEN 
                                                        CASE
                                                            WHEN jad.start_work_dttm IS NULL THEN TIME(DATE_FORMAT(DATE_ADD(a.appointment_dttm, INTERVAL 2 HOUR),'%%Y-%%m-%%d %%H:00:00'))
                                                            ELSE TIME(jad.end_work_dttm)
                                                        END
                                                    WHEN a.method = 'rscmku' THEN TIME(jad.end_work_dttm)
                                                    WHEN a.method = 'rawat_inap' THEN 
                                                        CASE
                                                            WHEN jad.start_work_dttm IS NULL THEN TIME(DATE_FORMAT(DATE_ADD(a.appointment_dttm, INTERVAL 2 HOUR),'%%Y-%%m-%%d %%H:00:00'))
                                                            ELSE TIME(jad.end_work_dttm)
                                                        END
                                                    ELSE TIME(DATE_FORMAT(DATE_ADD(a.appointment_dttm, INTERVAL 2 HOUR),'%%Y-%%m-%%d %%H:00:00'))
                                                END
                                        END AS DoctorPracticeFinishTime,
                                        '2' as VisitType,
                                        a.method AS AppointmentMethod,
                                        a.antrian AS QueueNumber,
                                        a.antrian as QueueNo,
                                        CONCAT(DATE(a.appointment_dttm),' ', TIME(a.appointment_dttm)) as ServedEstimated,
                                        CASE 
                                                WHEN a.appointment_id IS NULL THEN 
                                                        CASE 
                                                                WHEN a.payplan_id = 71 THEN 5
                                                                ELSE 0
                                                        END
                                                ELSE
                                                        CASE 
                                                                WHEN a.payplan_id = 71 THEN 
                                                                        CASE
                                                                                WHEN h.quota IS NULL THEN 0
                                                                                ELSE h.quota - h.used
                                                                        END
                                                                ELSE 0
                                                        END
                                        END AS RestQuotaJKN,
                                        CASE 
                                                WHEN a.appointment_id IS NULL THEN 
                                                        CASE
                                                                WHEN a.payplan_id = 71 THEN 50
                                                                ELSE 0
                                                        END
                                                ELSE
                                                        CASE 
                                                                WHEN a.payplan_id = 71 THEN 
                                                                        CASE
                                                                                WHEN h.quota IS NULL THEN 0
                                                                                ELSE h.quota
                                                                        END
                                                                ELSE 0
                                                        END
                                        END AS QuotaJKN,
                                        CASE 
                                                WHEN a.appointment_id IS NULL THEN 0
                                                ELSE
                                                        CASE
                                                                WHEN a.payplan_id <> 71 THEN 
                                                                        CASE
                                                                                WHEN h.quota IS NULL THEN 0
                                                                                ELSE h.quota - h.used
                                                                        END
                                                                ELSE 0
                                                        END
                                        END AS RestQuotaNonJKN,
                                        CASE 
                                                WHEN a.appointment_id IS NULL THEN 0
                                                ELSE
                                                        CASE
                                                                WHEN a.payplan_id <> 71 THEN 
                                                                        CASE
                                                                                WHEN h.quota IS NULL THEN 0
                                                                                ELSE h.quota
                                                                        END
                                                                ELSE 0
                                                        END
                                        END AS QuotaNonJKN,
                                        '-' as Notes
                                    from xocp_ehr_appointment a
                                    LEFT JOIN xocp_ehr_patient b on a.mrn = b.patient_ext_id
                                    LEFT JOIN xocp_persons c on c.person_id = b.person_id
                                    LEFT JOIN (SELECT rs_poli_cd,poli_cd,poli_nm from xocp_maping_poli_bpjs_bc2
                                    GROUP BY rs_poli_cd) d ON d.rs_poli_cd = a.org_id
                                    LEFT JOIN xocp_ehr_mapping_employee_doctorbpjs AS e ON e.employee_id = a.employee_id
                                    LEFT JOIN xocp_hrm_employee AS f ON f.employee_id = a.employee_id
                                    LEFT JOIN xocp_ehr_appointment_slot AS h ON a.appointment_slot_id = h.appointment_slot_id
                                    LEFT JOIN xocp_ehr_jadwal_dokter jad on jad.schedule_id = h.schedule_dr_id 
                                    WHERE 
                                    a.appointment_dttm >= DATE_FORMAT(NOW(), "%%Y-%%m-%%d 00:00:00")
                                    AND a.appointment_dttm <= DATE_FORMAT(NOW(), "%%Y-%%m-%%d 23:59:59")
                                    -- (a.appointment_dttm>= '2024-08-27 00:00:00' AND a.appointment_dttm <= '2024-08-27 23:59:59') 
                                    -- a.appointment_id IN (14737607)         
                                    AND a.org_id in (select CAST(org_id as int) as org_id from xocp_orgs where parent_id not in ('687','1872','2418') and org_id not in ('687','1872','2418'))
                                    
                                    and a.mrn NOT IN ('','10-00','10-01','10-02','10-03','10-04','10-05','10-06','10-07','10-08','10-09','10-10', '410-40-67',
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
                                
                                AND a.status_cd NOT IN ('cancelled','nullified')  
                                AND a.payplan_id = '71'     
                           order by a.appointment_id
                                """, con=conn_ehr)

source['Flag'] = '1'
# convert timedelta64[ns] ke time 
source['AppointmentTime'] = pd.to_datetime(source.AppointmentTime, unit='ns').dt.time
source['DoctorPracticeStartTime'] = pd.to_datetime(source.DoctorPracticeStartTime, unit='ns').dt.time
source['DoctorPracticeFinishTime'] = pd.to_datetime(source.DoctorPracticeFinishTime, unit='ns').dt.time

print(source)
source.replace({pd.NaT: None},inplace=True)
source.replace({np.nan: None},inplace=True)
source['QueueNumber'] = source['QueueNumber'].replace(np.nan,0)
source['QueueNumber']=source['QueueNumber'].astype('int64')
source['QueueNo'] = source['QueueNo'].replace(np.nan,0)
source['QueueNo']=source['QueueNo'].astype('int64')
source['PatientID']=source['PatientID'].astype('int64')
print(source.loc[:,['CardNo']])
# bikin function remove comma jika ada variable yang isinya cuma 1, variable ini akan di pakai IN CLAUSE di query target
def remove_comma(x):
    if len(x) == 1:
        return str(x).replace(',','')
    else:
        return x
        
# filter NIK yang kosong 
filtered_nik = source[source['NIK']=='-']
valid_nik = source[source['NIK']!= '-']

# bikin kondisi jika ada NIK yg kosong maka kita cari ke MPI
if not filtered_nik.empty:
    medicalno = tuple(filtered_nik['MedicalNo'].unique())
    medicalno = remove_comma(medicalno)
    
    query_cek = f""" SELECT 
                        PatientID as PatientIDNull,
                        NIK as NIKNull, 
                        MedicalNo 
                    FROM dwhrscm_talend.DimPatientMPI WHERE ScdActive='1'
                    AND PatientStatus = 'normal' AND MedicalNo IN {medicalno} 
                """
    source_mpi = pd.read_sql_query(query_cek,conn_dwh_sqlserver)
    
    # Join filtered_nik dengan NIK dan PatientID dari MPI
    filtered_nik = filtered_nik.merge(source_mpi,how='left',on='MedicalNo')
    
    # ubah PatientID dan NIK dari filtered_nik jadi ambil dari MPI
    filtered_nik['PatientID'] = filtered_nik['PatientIDNull']
    filtered_nik['NIK'] = filtered_nik['NIKNull']

    filtered_nik.drop(columns=['PatientIDNull','NIKNull'],inplace=True)

    # gabungkan filtered_nik dan valid_nik
    source = pd.concat([valid_nik,filtered_nik], ignore_index=True)
    # source = source.drop_duplicates(subset=['AppointmentID','PatientID','AdmissionID'])

    # ubah tipe data PatientID
    source['PatientID'] = source['PatientID'].replace(np.inf, np.nan)
    source['PatientID'] = source['PatientID'].fillna(0) 
    source['PatientID']=source['PatientID'].astype('int64')
    
# jika tidak ada NIK yang kosong maka tidak terjadi apa2, lanjut step berikutnya
else:
    pass

# filtered CardNo kosong
filtered_card_no = source[source['CardNo']=='-']
valid_card_no = source[source['CardNo']!= '-']

print('ini filter kartu kosong')
print(filtered_card_no)

# bikin kondisi jika ada CardNo yg kosong maka kita cari ke tabel PatientAdmission DWH
if not filtered_card_no.empty:
    patientid = tuple(filtered_card_no['PatientID'].unique())
    patientid = remove_comma(patientid)
    
    query_cek = f""" SELECT 
                        PatientID,
                        CardNoNull
                    FROM
                        (select
                            PatientID,
                            BPJSNo as CardNoNull,
                            DENSE_RANK()OVER(PARTITION BY PatientID ORDER BY BPJSNo DESC) as Ranked
                        from dwhrscm_talend.FactPatientAdmission
                        where PatientID IN {patientid}
                        and PayPlanID = '71'
                        and LEN(BPJSNo) = 13
                        GROUP BY PatientID,BPJSNo
                        ) x
                    WHERE x.Ranked = 1
                """
    source_admission = pd.read_sql_query(query_cek,conn_dwh_sqlserver)
    
    # Join filtered_card_no dengan CardNo dari Patient Admission DWH
    filtered_card_no = filtered_card_no.merge(source_admission,how='left',on='PatientID')
    
    # ubah PatientID dan NIK dari filtered_nik jadi ambil dari MPI
    filtered_card_no['CardNo'] = filtered_card_no['CardNoNull']

    filtered_card_no.drop(columns=['CardNoNull'],inplace=True)

    # gabungkan filtered_nik dan valid_nik
    source = pd.concat([valid_card_no,filtered_card_no], ignore_index=True)
    # source = source.drop_duplicates(subset=['AppointmentID','PatientID','AdmissionID'])
    
# jika tidak ada NIK yang kosong maka tidak terjadi apa2, lanjut step berikutnya
else:
    pass

print('ini source gabungan')
print(source.loc[:,['CardNo']])
# cek tipe data source
print(source.dtypes)

if source.empty:
    print('tidak ada data dari source')
else:
    # ambil primary key dari source, pake unique biar tidak duplicate
    appointmentid = tuple(source["AppointmentID"])
    patientid = tuple(source["PatientID"].unique())
    admissionid = tuple(source["AdmissionID"])
    
    appointmentid = remove_comma(appointmentid)
    patientid = remove_comma(patientid)
    admissionid = remove_comma(admissionid)
    
    # query buat narik data dari target lalu filter berdasarkan primary key
    query = f"SELECT AppointmentID,TRIM(KodeBooking) as KodeBooking,AdmissionDate,PatientID,AdmissionID,MedicalNo,PatientType,CardNo,SEPNo,ReferenceNo,NIK,PhoneNo,PoliCode,PoliName, NewPatient, TRIM(DoctorCode) AS DoctorCode,DoctorName,AppointmentDate,AppointmentTime,StatusAppointment,DoctorPracticeStartTime,DoctorPracticeFinishTime,VisitType,AppointmentMethod,QueueNumber,QueueNo,ServedEstimated,RestQuotaJKN,QuotaJKN,RestQuotaNonJKN,QuotaNonJKN,Notes,Flag FROM dwhrscm_talend.FactAppointmentNextDay where AppointmentID IN {appointmentid} AND PatientID IN {patientid} AND AdmissionID IN {admissionid} and Flag ='1' order by AppointmentID"
    target = pd.read_sql_query(query, conn_dwh_sqlserver)
  
    # replace value yang Nan jadi None
    target.replace({np.nan: None},inplace=True)
    # target['QueueNumber'] = target['QueueNumber'].replace(np.nan,0)
    # target['QueueNumber']=target['QueueNumber'].astype(int)
    # target['QueueNo'] = target['QueueNo'].replace(np.nan,0)
    # target['QueueNo']=target['QueueNo'].astype(int)
    print(target.dtypes)
    # source = source.loc[:,source.columns != 'SEPNoSpecial']
    print('after join')
    source['PatientID'] = source['PatientID'].astype('int64')
    source.replace({np.nan: None},inplace=True)
    target.replace({np.nan: None},inplace=True)
    
    
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