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
sys.stdout = open("C:/TestPython/DWH_SQL_Server/Staging/logs/LogTransAppointmentQueueEHR.txt","w")
t0 = time.time()

# bikin koneksi ke db
conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)
conn_dwh_mysql = db_connection.create_connection(db_connection.dwh_mysql)
conn_ehr = db_connection.create_connection(db_connection.replika_ehr)

# bikin variabel start_date dan end_date untuk memasukkan range tanggal awal dan akhir untuk diletakkan di WHERE query source 1,2,3
start_date = f"DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), '%%Y-%%m-%%d 00:00:00')"
end_date = f"DATE_FORMAT(NOW(), '%%Y-%%m-%%d 23:59:59')"

# start_date = f"'2024-04-25 00:00:00'"
# end_date = f"'2024-04-25 23:59:59'"

query_source = f""" WITH GetAdmission
AS 
(SELECT 
		CONCAT(a.patient_id,'-',a.admission_id) AS kode_booking,
		a.admission_dttm,
		DATE(a.admission_dttm) admission_date_only,
		a.patient_id,
		a.admission_id,
		b.patient_ext_id,
		a.org_id,
		CASE 
			WHEN a.payplan_id = 71 THEN 'JKN'
			ELSE 'Non JKN'
		END AS patient_type,
		a.payplan_attr,
		a.dr1,
		a.payplan_id
FROM xocp_ehr_patient_admission AS a
LEFT JOIN xocp_ehr_patient AS b ON b.patient_id = a.patient_id
WHERE 
(a.admission_dttm >= {start_date} AND a.admission_dttm <= {end_date}) 
AND a.status_cd NOT IN ('nullified','cancelled')
AND a.org_id in (select org_id from xocp_orgs where parent_id not in ('687','1872','2418') and org_id not in ('687','1872','2418'))
),
Appointment AS (
SELECT 
	CASE 
		WHEN d.appointment_id IS NULL THEN 0
		WHEN d.status_cd = 'cancelled' THEN 0
		WHEN d.status_cd = 'nullified' THEN 0
		ELSE d.appointment_id
	END AS AppointmentID,
	a.kode_booking as KodeBooking,
	a.admission_dttm as AdmissionDate,
    a.admission_date_only as AdmissionDateOnly,
	a.patient_id AS PatientID,
	a.admission_id as AdmissionID,
	a.patient_type as PatientType,
	a.payplan_attr as CardNo,
	CASE 
		WHEN d.appointment_id IS NULL THEN 
			CASE 
				WHEN i.poli_cd IS NULL THEN '-'
				ELSE i.poli_cd
			END
		ELSE 
			CASE
				WHEN h.poli_cd IS NULL THEN '-'
				ELSE h.poli_cd
			END
	END AS PoliCode,
	CASE 
		WHEN d.appointment_id IS NULL THEN 
			CASE 
				WHEN i.poli_nm IS NULL THEN '-'
				ELSE i.poli_nm
			END
		ELSE 
			CASE
				WHEN h.poli_nm IS NULL THEN '-'
				ELSE h.poli_nm
			END
	END AS PoliName,
	'0' AS NewPatient,
	CASE 
		WHEN d.appointment_id IS NULL THEN 
			CASE 
				WHEN j.doctorbpjs_id IS NULL THEN '-'
				ELSE j.doctorbpjs_id
			END
		ELSE 
			CASE 
				WHEN d.method = 'langsung' THEN
					CASE 
						WHEN e.doctorbpjs_id IS NULL THEN j.doctorbpjs_id
						ELSE e.doctorbpjs_id
					END
				WHEN d.method = 'rscmku' THEN e.doctorbpjs_id
				WHEN d.method = 'rawat_inap' THEN 
					CASE
						WHEN e.doctorbpjs_id IS NULL THEN j.doctorbpjs_id
						ELSE e.doctorbpjs_id
					END
				ELSE '-'
			END
	END AS DoctorCode,
	CASE 
		WHEN d.appointment_id IS NULL THEN
			CASE 
					WHEN k.person_nm IS NULL THEN '-'
					ELSE TRIM(k.person_nm) 
			END
		ELSE 
			CASE 
				WHEN d.method = 'langsung' THEN
					CASE 
						WHEN f.person_nm IS NULL THEN TRIM(k.person_nm)
						ELSE TRIM(f.person_nm)
					END
				WHEN d.method = 'rscmku' THEN TRIM(f.person_nm)
				WHEN d.method = 'rawat_inap' THEN 
					CASE
						WHEN f.person_nm IS NULL THEN TRIM(k.person_nm)
						ELSE TRIM(f.person_nm)
					END
				ELSE '-'
			END
	END AS DoctorName,
	CASE
		WHEN d.appointment_id IS NULL THEN DATE(a.admission_dttm) 
		ELSE DATE(d.appointment_dttm)
	END AS AppointmentDate,
	CASE
		WHEN d.appointment_id IS NULL THEN TIME(a.admission_dttm) 
		ELSE TIME(d.appointment_dttm)
	END AS AppointmentTime,
	CASE 
			-- WHEN d.appointment_id IS NULL THEN TIME(a.admission_dttm)
			WHEN d.appointment_id IS NULL THEN TIME(DATE_FORMAT(DATE_ADD(a.admission_dttm, INTERVAL 30 MINUTE),'%%Y-%%m-%%d %%H:00:00'))
			ELSE 
					CASE
							WHEN d.method = 'langsung' THEN 
									CASE
											WHEN jad.start_work_dttm IS NULL THEN TIME(DATE_FORMAT(DATE_ADD(d.appointment_dttm, INTERVAL 30 MINUTE),'%%Y-%%m-%%d %%H:00:00'))
											ELSE TIME(jad.start_work_dttm)
									END
							WHEN d.method = 'rscmku' THEN TIME(jad.start_work_dttm)
							WHEN d.method = 'rawat_inap' THEN 
									CASE
											WHEN jad.start_work_dttm IS NULL THEN TIME(DATE_FORMAT(DATE_ADD(d.appointment_dttm, INTERVAL 30 MINUTE),'%%Y-%%m-%%d %%H:00:00'))
											ELSE TIME(jad.start_work_dttm)
									END
							ELSE TIME(DATE_FORMAT(DATE_ADD(a.admission_dttm, INTERVAL 30 MINUTE),'%%Y-%%m-%%d %%H:00:00'))
					END
	END AS DoctorPracticeStartTime,
	CASE 
			WHEN d.appointment_id IS NULL THEN TIME(DATE_FORMAT(DATE_ADD(a.admission_dttm, INTERVAL 2 HOUR),'%%Y-%%m-%%d %%H:00:00'))
			ELSE 
					CASE
							WHEN d.method = 'langsung' THEN 
									CASE
											WHEN jad.start_work_dttm IS NULL THEN TIME(DATE_FORMAT(DATE_ADD(d.appointment_dttm, INTERVAL 2 HOUR),'%%Y-%%m-%%d %%H:00:00'))
											ELSE TIME(jad.end_work_dttm)
									END
							WHEN d.method = 'rscmku' THEN TIME(jad.end_work_dttm)
							WHEN d.method = 'rawat_inap' THEN 
									CASE
											WHEN jad.start_work_dttm IS NULL THEN TIME(DATE_FORMAT(DATE_ADD(d.appointment_dttm, INTERVAL 2 HOUR),'%%Y-%%m-%%d %%H:00:00'))
											ELSE TIME(jad.end_work_dttm)
									END
							ELSE TIME(DATE_FORMAT(DATE_ADD(d.appointment_dttm, INTERVAL 2 HOUR),'%%Y-%%m-%%d %%H:00:00'))
					END
	END AS DoctorPracticeFinishTime,
	'1' as VisitType,
	CASE
		WHEN d.appointment_id IS NULL THEN '-'
		ELSE 
			CASE
					WHEN d.method IS NULL THEN '-'
					ELSE d.method
			END 
	END AS AppointmentMethod,
	CASE 
		WHEN d.appointment_id IS NULL THEN 
			CASE 
				WHEN que.queue_num IS NULL THEN 0
				ELSE que.queue_num
			END
		ELSE 
			CASE
				WHEN d.antrian = 0 THEN que.queue_num
				ELSE d.antrian
			END
	END AS QueueNumber,
	CASE 
		WHEN d.appointment_id IS NULL THEN 
			CASE 
				WHEN que.queue_num IS NULL THEN 0
				ELSE que.queue_num
			END
		ELSE 
			CASE
				WHEN d.antrian = 0 THEN que.queue_num
				ELSE d.antrian
			END
	END AS QueueNo,
	'-' as ServedEstimated,
	CASE 
		WHEN d.appointment_id IS NULL THEN 
			CASE 
				WHEN a.payplan_id = 71 THEN 5
				ELSE 0
			END
		ELSE
			CASE 
				WHEN a.payplan_id = 71 THEN 
					CASE
							WHEN g.quota IS NULL THEN 0
							ELSE g.quota - g.used
					END
				ELSE 0
			END
	END AS RestQuotaJKN,
	CASE 
		WHEN d.appointment_id IS NULL THEN 
			CASE
				WHEN a.payplan_id = 71 THEN 50
				ELSE 0
			END
		ELSE
			CASE 
				WHEN a.payplan_id = 71 THEN 
					CASE
							WHEN g.quota IS NULL THEN 0
							ELSE g.quota
					END
				ELSE 0
			END
	END AS QuotaJKN,
	CASE 
		WHEN d.appointment_id IS NULL THEN 0
		ELSE
			CASE
				WHEN a.payplan_id <> 71 THEN 
					CASE
							WHEN g.quota IS NULL THEN 0
							ELSE g.quota - g.used
					END
				ELSE 0
			END
	END AS RestQuotaNonJKN,
	CASE 
		WHEN d.appointment_id IS NULL THEN 0
		ELSE
			CASE
				WHEN a.payplan_id <> 71 THEN 
					CASE
							WHEN g.quota IS NULL THEN 0
							ELSE g.quota
					END
				ELSE 0
			END
	END AS QuotaNonJKN,
    '-' AS Notes,
	DENSE_RANK() OVER(PARTITION BY a.patient_id,a.admission_id ORDER BY que.queue_num ASC ) RANK_QUE,
	DENSE_RANK() OVER(PARTITION BY a.patient_id,a.admission_id ORDER BY d.created_dttm ASC) RANK_APPOINTMENT,
	DENSE_RANK() OVER(PARTITION BY a.patient_id,a.admission_dttm ORDER BY a.admission_id DESC) RANK_BY_ADMISSION
FROM GetAdmission a
LEFT JOIN 
(SELECT 
	mrn, 
	method,
	employee_id,
	appointment_dttm,
	DATE(appointment_dttm) as appointment_date,
	appointment_id, 
	status_cd, 
	org_id,
	appointment_slot_id,
	antrian,
	created_dttm
	from xocp_ehr_appointment WHERE 
appointment_dttm >= {start_date} AND appointment_dttm <= {end_date}
) AS d ON d.mrn = a.patient_ext_id AND d.org_id = a.org_id AND d.appointment_date = a.admission_date_only AND d.status_cd='normal'
LEFT JOIN xocp_ehr_mapping_employee_doctorbpjs AS e ON d.employee_id = e.employee_id
LEFT JOIN xocp_hrm_employee AS f ON f.employee_id = d.employee_id
LEFT JOIN xocp_ehr_mapping_employee_doctorbpjs AS j ON a.dr1 = j.employee_id
LEFT JOIN xocp_hrm_employee AS k ON k.employee_id = a.dr1
LEFT JOIN xocp_ehr_appointment_slot AS g ON d.appointment_slot_id = g.appointment_slot_id
LEFT JOIN (select rs_poli_cd,poli_cd,poli_nm from xocp_maping_poli_bpjs_bc2
						GROUP BY rs_poli_cd) h ON h.rs_poli_cd = d.org_id
LEFT JOIN (select rs_poli_cd,poli_cd,poli_nm from xocp_maping_poli_bpjs_bc2
						GROUP BY rs_poli_cd) i on i.rs_poli_cd = a.org_id
LEFT JOIN xocp_ehr_jadwal_dokter jad on jad.schedule_id = g.schedule_dr_id 
LEFT JOIN xocp_ehr_patient_act act on act.patient_id = a.patient_id and act.admission_id = a.admission_id and act.dest_org_id = a.org_id
LEFT JOIN xocp_ehr_queue_policlinic que on que.act_id = act.act_id and que.patient_id = act.patient_id and que.admission_id = que.admission_id
)
SELECT 
	AppointmentID,
	KodeBooking,
	AdmissionDate,
    AdmissionDateOnly,
	PatientID,
	AdmissionID,
	PatientType,
	CardNo,
	PoliCode,
	PoliName,
	NewPatient,
	DoctorCode,
	DoctorName,
	AppointmentDate,
	AppointmentTime,
	DoctorPracticeStartTime,
	DoctorPracticeFinishTime,
	VisitType,
	AppointmentMethod,
	QueueNumber,
	QueueNo,
	ServedEstimated,
	RestQuotaJKN,
	QuotaJKN,
	RestQuotaNonJKN,
	QuotaNonJKN,
    Notes
FROM Appointment
WHERE RANK_QUE = 1 AND RANK_APPOINTMENT = 1 AND RANK_BY_ADMISSION = 1 
GROUP BY
AppointmentID,
	KodeBooking,
	AdmissionDate,
	PatientID,
	AdmissionID,
	PatientType,
	CardNo,
	PoliCode,
	PoliName,
	NewPatient,
	DoctorCode,
	DoctorName,
	AppointmentDate,
	AppointmentTime,
	DoctorPracticeStartTime,
	DoctorPracticeFinishTime,
	VisitType,
	AppointmentMethod,
	QueueNumber,
	QueueNo,
	ServedEstimated,
	RestQuotaJKN,
	QuotaJKN,
	RestQuotaNonJKN,
	QuotaNonJKN,
    Notes"""
source= pd.read_sql_query(query_source,conn_ehr)

# ambil semua kolom kecuali kolom Rank

source['Flag'] = '1'
# convert timedelta64[ns] ke time 
source['AppointmentTime'] = pd.to_datetime(source.AppointmentTime, unit='ns').dt.time
source['DoctorPracticeStartTime'] = pd.to_datetime(source.DoctorPracticeStartTime, unit='ns').dt.time
source['DoctorPracticeFinishTime'] = pd.to_datetime(source.DoctorPracticeFinishTime, unit='ns').dt.time

print(source)
source.replace({pd.NaT: None},inplace=True)
source.replace({np.nan: None},inplace=True)
source['QueueNumber'] = source['QueueNumber'].replace(np.nan,0)
source['QueueNumber']=source['QueueNumber'].astype(int)
source['QueueNo'] = source['QueueNo'].replace(np.nan,0)
source['QueueNo']=source['QueueNo'].astype(int)

# cek tipe data source
print(source.dtypes)

query_get_sep_rujukan = f""" SELECT * FROM
                                (SELECT 
                                    NomorKartu as CardNo, 
                                    NomorSep as SEPNo,
                                    NomorRujukan as ReferenceNo, 
                                    DATE(TanggalKirim) as AdmissionDateOnly,
                                    DENSE_RANK()OVER(PARTITION BY DATE(TanggalKirim),NomorKartu ORDER BY TanggalKirim DESC) as rank
                                FROM FactBPJSDataSent
                                where TitleSend = 'sep cron'
                                ) x
                                WHERE x.rank = 1
                        """
dwh_get_sep_rujukan = pd.read_sql_query(query_get_sep_rujukan, conn_dwh_mysql)
print(dwh_get_sep_rujukan.dtypes)
print(dwh_get_sep_rujukan)
source=source.merge(dwh_get_sep_rujukan,how='left',on=['AdmissionDateOnly','CardNo'])

new_order_columns = ['AppointmentID','KodeBooking','AdmissionDate','PatientID','AdmissionID','PatientType',
                    'CardNo','SEPNo','ReferenceNo','PoliCode','PoliName','NewPatient','DoctorCode',
                    'DoctorName','AppointmentDate','AppointmentTime','DoctorPracticeStartTime','DoctorPracticeFinishTime',
                    'VisitType','AppointmentMethod','QueueNumber','QueueNo',
                    'ServedEstimated','RestQuotaJKN','QuotaJKN','RestQuotaNonJKN','QuotaNonJKN','Notes','Flag']
source = source.reindex(columns=new_order_columns)

# source['SEPNo'].fillna(source['SEPNoSpecial'],inplace=True)
# semua yang null dari source diganti jadi strip '-' karenada SEP dan rujukan yg null
# source.replace({np.nan: '-'},inplace=True)
source['SEPNo'] = source['SEPNo'].fillna('-')
source['ReferenceNo'] = source['ReferenceNo'].fillna('-')

if source.empty:
    print('tidak ada data dari source')
else:
    appointmentid = tuple(source["AppointmentID"])
    patientid = tuple(source["PatientID"].unique())
    admissionid = tuple(source["AdmissionID"].unique())
    if len(appointmentid) > 1:
        pass
    else:
        appointmentid = str(appointmentid).replace(',','')       
    if len(patientid) > 1:
        pass
    else:
        patientid = str(patientid).replace(',','')

    if len(admissionid) > 1:
        pass
    else:
        admissionid = str(admissionid).replace(',','') 
    
    # query buat narik data dari target lalu filter berdasarkan primary key
    query = f"SELECT AppointmentID,TRIM(KodeBooking) as KodeBooking,AdmissionDate,PatientID,AdmissionID,PatientType,CardNo,SEPNo,ReferenceNo,PoliCode,PoliName, NewPatient, TRIM(DoctorCode) AS DoctorCode,DoctorName,AppointmentDate,AppointmentTime,DoctorPracticeStartTime,DoctorPracticeFinishTime,VisitType,AppointmentMethod,QueueNumber,QueueNo,ServedEstimated,RestQuotaJKN,QuotaJKN,RestQuotaNonJKN,QuotaNonJKN,Notes,Flag FROM staging_rscm.TransAppointmentQueue where AppointmentID IN {appointmentid} AND PatientID IN {patientid} AND AdmissionID IN {admissionid} and Flag ='1' order by AppointmentID"
    target = pd.read_sql_query(query, conn_staging_sqlserver)
    
    # replace value yang Nan jadi None
    target.replace({np.nan: None},inplace=True)

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
        inserted['InsertDateStaging'] = today_convert
        inserted.to_sql('TransAppointmentQueue', schema='staging_rscm', con = conn_staging_sqlserver, if_exists = 'append', index=False)
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
            df.to_sql(temp_table,schema = 'staging_rscm',con=conn_staging_sqlserver, if_exists='replace',index=False)
            update_stmt_1 = f'UPDATE r '
            update_stmt_2 = f'SET '
            update_stmt_3 = ", ".join(list_col)
            update_stmt_8 = f' , r.UpdateDateStaging = CONVERT(DATETIME2(0), GETDATE(),120)'
            update_stmt_4 = f' FROM staging_rscm.{table} r '
            update_stmt_5 = f'INNER JOIN (SELECT * FROM staging_rscm.{temp_table}) as t ON r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} AND r.{pk_3} = t.{pk_3} AND r.{pk_4} = t.{pk_4} '
            update_stmt_6 = f'WHERE r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} AND r.{pk_3} = t.{pk_3} AND r.{pk_4} = t.{pk_4} '
            update_stmt_7 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_8 + update_stmt_4 + update_stmt_5 + update_stmt_6 +";"
            delete_stmt_1 = f'DROP TABLE staging_rscm.{temp_table}'
            print(update_stmt_7)
            conn_staging_sqlserver.execute(update_stmt_7)
            conn_staging_sqlserver.execute(delete_stmt_1)

        try:
            # update data
            updated_to_sql(modified, 'TransAppointmentQueue', 'AppointmentID','PatientID','AdmissionID','Flag')

            # insert data baru
            today = dt.datetime.now()
            today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
            inserted['InsertDateStaging'] = today_convert
            inserted.to_sql('TransAppointmentQueue', schema='staging_rscm', con=conn_staging_sqlserver, if_exists ='append',index=False)
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
db_connection.close_connection(conn_staging_sqlserver)