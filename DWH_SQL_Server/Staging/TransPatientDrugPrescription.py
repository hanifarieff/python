import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pyodbc
import pandas as pd
import time
import datetime as dt
date = dt.datetime.today()

# bikin log ke file
sys.stdout =  open("C:/TestPython/DWH_SQL_Server/Staging/logs/LogTransPatientDrugPrescription.txt","w")
t0 = time.time()

# bikin koneksi ke db
conn_ehr = db_connection.create_connection(db_connection.replika_ehr)
conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)

source = pd.read_sql_query(""" SELECT
                                    a.prescription_id as PrescriptionID, -- char(15)
                                    b.item_id as SequenceID, -- int(11)
                                    a.patient_id as PatientID, -- int(10)
                                    a.admission_id as AdmissionID, -- int(10)
                                    a.presc_org_id as PrescriptionOrgID,
                                    b.obj_id as DrugID, -- char(20)
                                    c.obj_nm as DrugName,
                                    b.obj_qty as DrugQuantity, -- char(30)
                                    CASE
                                        WHEN b.freq_cd = 1 THEN '1 kali per hari'
                                        WHEN b.freq_cd = 2 THEN '2 kali per hari'
                                        WHEN b.freq_cd = 3 THEN '3 kali per hari'
                                        WHEN b.freq_cd = 4 THEN '4 kali per hari'
                                        WHEN b.freq_cd = 5 THEN 'tiap jam'
                                        WHEN b.freq_cd = 6 THEN 'tiap 2 jam'
                                        WHEN b.freq_cd = 7 THEN 'tiap 3 jam'
                                        WHEN b.freq_cd = 8 THEN 'tiap 4 jam'
                                        WHEN b.freq_cd = 9 THEN 'tiap 6 jam'
                                        WHEN b.freq_cd = 10 THEN 'tiap 8 jam'
                                        WHEN b.freq_cd = 11 THEN 'tiap 12 jam'
                                        WHEN b.freq_cd = 12 THEN 'tiap 24 jam'
                                        WHEN b.freq_cd = 13 THEN 'selang sehari'
                                        WHEN b.freq_cd = 14 THEN '1 kali per minggu'
                                        WHEN b.freq_cd = 15 THEN 'setiap 2 minggu'
                                        WHEN b.freq_cd = 16 THEN 'setiap 28 hari'
                                        WHEN b.freq_cd = 17 THEN 'setiap 30 hari'
                                        WHEN b.freq_cd = 18 THEN 'bila perlu'
                                        WHEN b.freq_cd = 19 THEN '1 kali'
                                        WHEN b.freq_cd = 20 THEN '5 kali per hari'
                                        WHEN b.freq_cd = 21 THEN '6 kali per hari'
                                        WHEN b.freq_cd = 23 THEN 'tiap 72 jam'
                                        ELSE b.freq_cd
                                    END AS FrequencyCode, -- varchar(255)
                                    b.unit_cd as UnitCode, -- char(15)
                                    b.x_qty as DispenseQuantity, -- char(30)
                                    b.x_unit_cd as DispenseUnitCode, -- char(15)
                                    CASE
                                        WHEN forn.OBAT IS NULL THEN 0
                                        ELSE 1
                                    END AS IsFornas,
                                    emp.employee_id as CreatedID,
		                            a.dr_id as DoctorID,
                                    a.created_dttm as DrugPrescriptionDate,
                                    a.prescription_note as PrescriptionNotes,
                                    a.status_cd as PrescriptionStatus
                                FROM
                                xocp_ehr_prescription AS a
                                inner JOIN xocp_ehr_prescription_item AS b ON a.prescription_id = b.prescription_id
                                left join xocp_ehr_obj c on b.obj_id = c.obj_id
                                LEFT JOIN xocp_users usr on usr.user_id = a.created_user_id
                                LEFT JOIN xocp_hrm_employee emp on emp.person_id = usr.person_id
                                LEFT JOIN FORNAS_BRANDS forn on c.obj_id = forn.obj_id
                                where admission_id <> '0' and 
                                -- a.created_dttm >= '2024-09-04 00:00:00' AND a.created_dttm <= '2024-09-05 23:59:59'
                                -- AND a.patient_id = 1678877 AND a.admission_id = 9
                                a.created_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 3 DAY), "%%Y-%%m-%%d 00:00:00") AND a.created_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 23:59:59")                                
                                -- AND a.prescription_id IN ('00150000472918','00150000472917')     
                           """, conn_ehr)

print(source.iloc[:,0:8])
source['DoctorID'] = source['DoctorID'].astype('int64')
source['IsFornas']=source['IsFornas'].astype('int64')

if source.empty:
    print('tidak ada data dari source')
else:
    prescriptionid = tuple(source["PrescriptionID"].unique())
    sequenceid = tuple(source["SequenceID"].unique())

    # bikin function remove comma jika ada variable yang isinya cuma 1, variable ini akan di pakai IN CLAUSE di query target
    def remove_comma(x):
        if len(x) == 1:
            return str(x).replace(',','')
        else:
            return x
    
    prescriptionid = remove_comma(prescriptionid)
    sequenceid = remove_comma(sequenceid)

    # query buat narik data dari target lalu filter berdasarkan primary key
    query = f'SELECT TRIM(PrescriptionID) as PrescriptionID,SequenceID,PatientID,AdmissionID,PrescriptionOrgID,DrugID,DrugName,DrugQuantity,FrequencyCode,UnitCode,DispenseQuantity,DispenseUnitCode,IsFornas,CreatedID,DoctorID,DrugPrescriptionDate,PrescriptionNotes,PrescriptionStatus from staging_rscm.TransPatientDrugPrescription where PrescriptionID IN {prescriptionid} AND SequenceID IN {sequenceid} order by PrescriptionID,SequenceID'
    target = pd.read_sql_query(query, conn_staging_sqlserver)
    
    target['IsFornas'] = pd.to_numeric(target['IsFornas'], errors='coerce')
    target['IsFornas'].fillna(3,inplace=True)
    target['IsFornas'] = target['IsFornas'].astype('int64')
 
    # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

    # ambil data yang update dari changes
    modified = changes[changes[['PrescriptionID','SequenceID']].apply(tuple,1).isin(target[['PrescriptionID','SequenceID']].apply(tuple,1))]
    total_row_upd = len(modified)
    text_upd = f'total row update : {total_row_upd}'
    print(text_upd)
    print(modified)

    # ambil data yang new dari changes
    inserted = changes[~changes[['PrescriptionID','SequenceID']].apply(tuple,1).isin(target[['PrescriptionID','SequenceID']].apply(tuple,1))]
    total_row_ins = len(inserted)
    text_ins = f'total row inserted : {total_row_ins}'
    print(text_ins)
    print(inserted)

    if modified.empty:
        if inserted.empty:
            print('there is no data updated and inserted')
        else :
            # bikin tanggal sekarang buat kolom InsertDateStaging
            today = dt.datetime.now()
            today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
            inserted['InsertDateStaging'] = today_convert
            inserted.to_sql('TransPatientDrugPrescription', schema='staging_rscm', con=conn_staging_sqlserver, if_exists = 'append', index=False)
            print('success insert all data without update')
    
    else:
        # buat fungsi untuk update data ke tabel target
        def updated_to_sql(df, table_name, key_1,key_2):
            list_col = []
            table=table_name
            pk_1 = key_1
            pk_2 = key_2
            temp_table = f'{table}_temporary_table'
            for col in df.columns:
                if col == pk_1 or col == pk_2 :
                    continue
                list_col.append(f'r.{col} = t.{col}')
            df.to_sql(temp_table, schema= 'staging_rscm',con=conn_staging_sqlserver, if_exists='replace',index=False)
            update_stmt_1 = f'UPDATE r '
            update_stmt_2 = f'SET '
            update_stmt_3 = ", ".join(list_col)
            update_stmt_8 = f' , r.UpdateDateStaging = CONVERT(DATETIME2(0), GETDATE(),120)'
            update_stmt_4 = f' FROM staging_rscm.{table} r '
            update_stmt_5 = f'INNER JOIN (SELECT * FROM staging_rscm.{temp_table}) as t ON r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} '  
            update_stmt_6 = f' WHERE r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} '
            update_stmt_7 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_8 + update_stmt_4 + update_stmt_5 + update_stmt_6 +";"
            delete_stmt_1 = f'DROP TABLE staging_rscm.{temp_table}'
            print(update_stmt_7)
            conn_staging_sqlserver.execute(update_stmt_7)
            conn_staging_sqlserver.execute(delete_stmt_1)

        try:
            # update data
            updated_to_sql(modified, 'TransPatientDrugPrescription', 'PrescriptionID', 'SequenceID')

            #insert data baru
            today = dt.datetime.now()
            today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
            inserted['InsertDateStaging'] = today_convert
            inserted.to_sql('TransPatientDrugPrescription', schema='staging_rscm',con=conn_staging_sqlserver, if_exists ='append',index=False)
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
db_connection.close_connection(conn_staging_sqlserver)