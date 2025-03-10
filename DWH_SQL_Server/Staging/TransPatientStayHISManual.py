import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pandas as pd
import pyodbc
import datetime as dt
date = dt.datetime.today()
import time

# bikin log ke file
sys.stdout = open("C:/TestPython/DWH_SQL_Server/Staging/logs/LogTransPatientStayNewHIS.txt","w")
t0 = time.time()


conn_his_live = db_connection.create_connection(db_connection.his_live)
conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)

# bikin variabel start_date dan end_date untuk memasukkan range tanggal awal dan akhir untuk diletakkan di WHERE query source 1,2,3
start_date = f"DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 2 DAY), '%%Y-%%m-%%d 00:00:00')"
end_date = f"DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), '%%Y-%%m-%%d 23:59:59')"

# # bikin variabel start_date dan end_date untuk memasukkan range tanggal awal dan akhir untuk diletakkan di WHERE query source 1,2,3
# start_date = f"'2024-05-21 00:00:00'"
# end_date = f"'2024-05-25 23:59:59'"

# tarik query, masuk ke variabel source
query_source1 = f""" SELECT DISTINCT
                                    adm.patient_id as PatientID,
                                    adm.admission_id as AdmissionID,
                                    p.patient_mrn_txt AS MedicalNo,
                                    adm.admission_dttm as AdmissionDate,
                                    CASE
                                        WHEN x.patient_id IS NULL THEN 'n'
                                        ELSE 'y'
                                    END AS StayInd,
                                    CASE
                                        WHEN adm.admission_type = 'outpatient' THEN 'n'
                                        WHEN adm.admission_type = 'inpatient' THEN 'y'
                                    END AS StayIndAdmission
                                FROM xocp_his_patient_admission adm
                                LEFT JOIN xocp_his_patient p ON p.patient_id = adm.patient_id
                                LEFT JOIN 
                                (
                                    SELECT 
                                        a.patient_id,
                                        a.admission_id,
                                        a.location_id,
                                        a.created_dttm,
                                        a.updated_dttm
                                    FROM xocp_his_patient_location a 
                                    LEFT JOIN xocp_location b on a.location_id = b.location_id 
                                    WHERE a.status_cd NOT IN ('nullified') AND b.sk_count = '1'
                                -- 	AND a.patient_id = 994340 and a.admission_id = 3
                                    ORDER BY patient_id,admission_id
                                ) x ON x.patient_id = adm.patient_id and x.admission_id = adm.admission_id
                                WHERE adm.org_id in (select org_id from xocp_orgs where parent_id in ('687','1872','2418') OR org_id in ('687','1872','2418'))
                                -- AND adm.status_cd NOT IN ('nullified','cancelled')
                                AND adm.admission_dttm >= '2025-02-21 00:00:00' AND adm.admission_dttm <= '2025-02-25 23:59:59'
                                -- AND adm.patient_id IN (605123,1120633) and adm.admission_id IN (82,42)
                                -- AND adm.admission_dttm >= {start_date} AND adm.admission_dttm <= {end_date}
                        
                                 """
source1 = pd.read_sql_query(query_source1,conn_his_live)
print(source1)

# query_source2 = f""" SELECT DISTINCT
#                                     adm.patient_id as PatientID,
#                                     adm.admission_id as AdmissionID,
#                                     p.patient_mrn_txt AS MedicalNo,
#                                     adm.admission_dttm as AdmissionDate,
#                                     CASE
#                                         WHEN x.patient_id IS NULL THEN 'n'
#                                         ELSE 'y'
#                                     END AS StayInd,
#                                       CASE
#                                         WHEN adm.admission_type = 'outpatient' THEN 'n'
#                                         WHEN adm.admission_type = 'inpatient' THEN 'y'
#                                     END AS StayIndAdmission
#                                 FROM xocp_his_patient_admission adm
#                                 LEFT JOIN xocp_his_patient p ON p.patient_id = adm.patient_id
#                                 LEFT JOIN 
#                                 (
#                                     SELECT 
#                                         a.patient_id,
#                                         a.admission_id,
#                                         a.location_id,
#                                         a.created_dttm,
#                                         a.updated_dttm
#                                     FROM xocp_his_patient_location a 
#                                     LEFT JOIN xocp_location b on a.location_id = b.location_id 
#                                     WHERE a.status_cd NOT IN ('nullified') AND b.sk_count = '1'
#                                 -- 	AND a.patient_id = 994340 and a.admission_id = 3
#                                     ORDER BY patient_id,admission_id
#                                 ) x ON x.patient_id = adm.patient_id and x.admission_id = adm.admission_id
#                                 WHERE adm.org_id in (select org_id from xocp_orgs where parent_id in ('687','1872','2418') OR org_id in ('687','1872','2418'))
#                                 AND adm.status_cd NOT IN ('nullified','cancelled')
#                                 -- AND adm.admission_dttm >= '2023-12-25 00:00:00' AND adm.admission_dttm <= '2023-12-27 23:59:59'
#                                 AND 
#                                 x.created_dttm >= {start_date} AND x.created_dttm <= {end_date}
                        
#                                  """
# source2 = pd.read_sql_query(query_source2,conn_his_live)
# print(source2)

# query_source3 = f""" SELECT DISTINCT
#                                     adm.patient_id as PatientID,
#                                     adm.admission_id as AdmissionID,
#                                     p.patient_mrn_txt AS MedicalNo,
#                                     adm.admission_dttm as AdmissionDate,
#                                     CASE
#                                         WHEN x.patient_id IS NULL THEN 'n'
#                                         ELSE 'y'
#                                     END AS StayInd,
#                                       CASE
#                                         WHEN adm.admission_type = 'outpatient' THEN 'n'
#                                         WHEN adm.admission_type = 'inpatient' THEN 'y'
#                                       END AS StayIndAdmission
#                                 FROM xocp_his_patient_admission adm
#                                 LEFT JOIN xocp_his_patient p ON p.patient_id = adm.patient_id
#                                 LEFT JOIN 
#                                 (
#                                     SELECT 
#                                         a.patient_id,
#                                         a.admission_id,
#                                         a.location_id,
#                                         a.created_dttm,
#                                         a.updated_dttm
#                                     FROM xocp_his_patient_location a 
#                                     LEFT JOIN xocp_location b on a.location_id = b.location_id 
#                                     WHERE a.status_cd NOT IN ('nullified') AND b.sk_count = '1'
#                                 -- 	AND a.patient_id = 994340 and a.admission_id = 3
#                                     ORDER BY patient_id,admission_id
#                                 ) x ON x.patient_id = adm.patient_id and x.admission_id = adm.admission_id
#                                 WHERE adm.org_id in (select org_id from xocp_orgs where parent_id in ('687','1872','2418') OR org_id in ('687','1872','2418'))
#                                 AND adm.status_cd NOT IN ('nullified','cancelled')
#                                 -- AND adm.admission_dttm >= '2023-12-25 00:00:00' AND adm.admission_dttm <= '2023-12-27 23:59:59'
#                                 AND 
#                                 x.updated_dttm >= {start_date} AND x.updated_dttm <= {end_date}
#                                 """

source = source1

# # source = source1
# source = pd.concat([source1,source2,source3], ignore_index=True)
# source = source.drop_duplicates(subset=['PatientID','AdmissionID'])

source['Flag'] = 2
source['Flag']=source['Flag'].astype(str)

print(source)

if source.empty:
    print('tidak ada data dari source')
else:
    patientid = tuple(source["PatientID"]) 
    admissionid = tuple(source["AdmissionID"])

     # bikin function remove comma jika ada variable yang isinya cuma 1, variable ini akan di pakai IN CLAUSE di query target
    def remove_comma(x):
        if len(x) == 1:
            return str(x).replace(',','')
        else:
            return x
    
    patientid = remove_comma(patientid)
    admissionid = remove_comma(admissionid)

    # query buat narik data dari target lalu filter berdasarkan primary key
    query = f'SELECT PatientID,AdmissionID,MedicalNo,AdmissionDate,StayInd,StayIndAdmission,Flag from staging_rscm.TransPatientStay where PatientID IN {patientid} AND AdmissionID IN {admissionid} order by PatientID, AdmissionID'
    target = pd.read_sql_query(query, conn_staging_sqlserver)


    
# ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]
# print(changes)

# ambil data yang update
modified = changes[changes[['PatientID','AdmissionID']].apply(tuple,1).isin(target[['PatientID','AdmissionID']].apply(tuple,1))]
total_row_upd = len(modified)
text_upd = f'total row update : {total_row_upd}'
print(text_upd)
print(modified)

#ambil data yang baru
inserted = changes[~changes[['PatientID','AdmissionID']].apply(tuple,1).isin(target[['PatientID','AdmissionID']].apply(tuple,1))]
total_row_ins = len(inserted)
text_ins = f'total row inserted : {total_row_ins}'
print(text_ins)
print(inserted)

if modified.empty:
    if inserted.empty:
        print('there is no data updated and inserted')
    else:
        today = dt.datetime.now()
        today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
        inserted['InsertDateStaging'] = today_convert
        inserted.to_sql('TransPatientStay', schema='staging_rscm', con = conn_staging_sqlserver, if_exists='append',index=False)
        print('success insert without update')
else:
    # buat fungsi untuk update
    def updated_to_sql(df, table_name, key_1, key_2):
        list_col = []
        table = table_name
        pk_1 = key_1
        pk_2 = key_2
        temp_table = f'{table}_temporary_table'
        for col in df.columns:
            if col == pk_1 or col == pk_2 :
                continue
            list_col.append(f't.{col} = s.{col}')
        df.to_sql(temp_table, schema = 'staging_rscm', con = conn_staging_sqlserver, if_exists='replace',index = False)
        update_stmt_1 = f'UPDATE t '
        update_stmt_2 = f'SET '
        update_stmt_3 = ", ".join(list_col)
        update_stmt_8 = f' , t.UpdateDateStaging = CONVERT(DATETIME2(0), GETDATE(),120)'
        update_stmt_4 = f' FROM staging_rscm.{table} t '
        update_stmt_5 = f'INNER JOIN (SELECT * from staging_rscm.{temp_table}) AS s ON t.{pk_1} = s.{pk_1} AND t.{pk_2} = s.{pk_2} '
        update_stmt_6 = f'WHERE t.{pk_1} = s.{pk_1} AND t.{pk_2} = s.{pk_2} '
        update_stmt_7 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_8 + update_stmt_4 + update_stmt_5 +  update_stmt_6 +";"
        delete_stmt_1 = f'DROP TABLE staging_rscm.{temp_table}'
        print(update_stmt_7)
        print('\n')
        conn_staging_sqlserver.execute(update_stmt_7)
        conn_staging_sqlserver.execute(delete_stmt_1)

    try:
        # call fungsi update
        updated_to_sql(modified, 'TransPatientStay', 'PatientID','AdmissionID')
        # masukkan data yang baru ke table target
        today = dt.datetime.now()
        today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
        inserted['InsertDateStaging'] = today_convert
        inserted.to_sql('TransPatientStay', schema='staging_rscm', con = conn_staging_sqlserver, if_exists='append',index=False)
        print('success update dan insert')
    except Exception as e:
        print(e)

t1 = time.time()
total=t1-t0
print(total)
text=f'scheduler tanggal : {date}'
print(text)

db_connection.close_connection(conn_his_live)
db_connection.close_connection(conn_staging_sqlserver)