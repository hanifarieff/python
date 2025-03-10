import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pandas as pd
import pyodbc
import datetime as dt
date = dt.datetime.today()
import time

# bikin log ke file
sys.stdout = open("C:/TestPython/DWH_SQL_Server/Staging/logs/LogTransPatientStayNewEHR.txt","w")
text=f'scheduler tanggal : {date}'
print(text)
t0 = time.time()

# bikin koneksi ke db
conn_ehr_live = db_connection.create_connection(db_connection.ehr_live)
conn_ehr = db_connection.create_connection(db_connection.replika_ehr)
conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)

# bikin variabel start_date dan end_date untuk memasukkan range tanggal awal dan akhir untuk diletakkan di WHERE query source 1,2,3
start_date = f"DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 2 DAY), '%%Y-%%m-%%d 00:00:00')"
end_date = f"DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), '%%Y-%%m-%%d 23:59:59')"

# start_date = f"'2024-05-30 00:00:00'"
# end_date = f"'2024-05-31 23:59:59'"


# source1 filter berdasarkan kolom admission_dttm
query_source1 = f""" SELECT 
                            x.PatientID,
                            x.AdmissionID,
                            x.MedicalNo,
                            x.AdmissionDate,
                            CASE 
                                WHEN GROUP_CONCAT(x.mark_stay) LIKE '%%y%%' THEN 'y'
                                ELSE 'n'
                            END AS StayInd,
                            x.stay_ind as StayIndAdmission
                        FROM
                        (
                            SELECT
                                adm.patient_id as PatientID,
                                adm.admission_id as AdmissionID,
                                p.patient_ext_id as MedicalNo,
                                adm.admission_dttm as AdmissionDate,
                                st.in_org_id,
                                st.location_id,
                                b.sensus,
                                CASE 
                                    WHEN b.sensus IS NOT NULL THEN 
                                        CASE
                                            WHEN st.in_org_id = 104 then 'n'
                                            ELSE 'y'
                                        END
                                    ELSE 'n'
                                END AS mark_stay,
                                CASE
                                    WHEN TIMEDIFF(st.stop_dttm, st.start_dttm) LIKE '%%-%%' THEN '00:00:00'
                                    ELSE TIMEDIFF(st.stop_dttm, st.start_dttm)
                                END AS hours_of_stay,
                                st.start_dttm,
                                st.stop_dttm,
                                st.created_dttm,
                                st.updated_dttm,
                                adm.stay_ind
                            FROM xocp_ehr_patient_admission adm 
                            LEFT JOIN xocp_ehr_patient p on p.patient_id = adm.patient_id
                            LEFT JOIN xocp_ehr_patient_stay st on adm.patient_id = st.patient_id and adm.admission_id = st.admission_id and st.status_cd NOT IN ('nullified','reserved') 
                            LEFT JOIN xocp_ehr_location b on st.location_id = b.location_id AND st.in_org_id = b.org_id
                            AND b.sensus = '1'
                            WHERE 
                            -- adm.patient_id IN (1765541) and adm.admission_id IN (1)
                            (adm.admission_dttm >= '2021-12-21 00:00:00' AND adm.admission_dttm <= '2021-12-31 23:59:59')
                            -- AND adm.patient_id = 1649519 and adm.admission_id = 11 
                            -- adm.admission_dttm >= {start_date} AND adm.admission_dttm <= {end_date}
                            AND (adm.org_id IN (select org_id from xocp_orgs where parent_id not in ('687','1872','2418') and org_id not in ('687','1872','2418'))
                            OR adm.org_id = '0')
                            -- AND adm.status_cd NOT IN ('cancelled','nullified') 
                            ORDER BY adm.patient_id, adm.admission_id
                        ) x
                        GROUP BY x.PatientID, x.AdmissionID"""
source1 = pd.read_sql_query(query_source1, conn_ehr)
print(source1)

# # source2 filter berdasarkan kolom start_dttm
# query_source2 = f""" SELECT 
#                             x.PatientID,
#                             x.AdmissionID,
#                             x.MedicalNo,
#                             x.AdmissionDate,
#                             CASE 
#                                 WHEN GROUP_CONCAT(x.mark_stay) LIKE '%%y%%' THEN 'y'
#                                 ELSE 'n'
#                             END AS StayInd,
#                             x.stay_ind as StayIndAdmission
#                         FROM
#                         (
#                             SELECT
#                                 adm.patient_id as PatientID,
#                                 adm.admission_id as AdmissionID,
#                                 p.patient_ext_id as MedicalNo,
#                                 adm.admission_dttm as AdmissionDate,
#                                 st.in_org_id,
#                                 st.location_id,
#                                 b.sensus,
#                                 CASE 
#                                     WHEN b.sensus IS NOT NULL THEN 
#                                         CASE
#                                             WHEN st.in_org_id = 104 then 'n'
#                                             ELSE 'y'
#                                         END
#                                     ELSE 'n'
#                                 END AS mark_stay,
#                                 CASE
#                                     WHEN TIMEDIFF(st.stop_dttm, st.start_dttm) LIKE '%%-%%' THEN '00:00:00'
#                                     ELSE TIMEDIFF(st.stop_dttm, st.start_dttm)
#                                 END AS hours_of_stay,
#                                 st.start_dttm,
#                                 st.stop_dttm,
#                                 st.created_dttm,
#                                 st.updated_dttm,
#                                 adm.stay_ind
#                             FROM xocp_ehr_patient_admission adm 
#                             LEFT JOIN xocp_ehr_patient p on p.patient_id = adm.patient_id
#                             LEFT JOIN xocp_ehr_patient_stay st on adm.patient_id = st.patient_id and adm.admission_id = st.admission_id and st.status_cd NOT IN ('nullified','reserved') 
#                             LEFT JOIN xocp_ehr_location b on st.location_id = b.location_id AND st.in_org_id = b.org_id
#                             AND b.sensus = '1'
#                             WHERE 
#                             -- adm.patient_id IN (2077816) and adm.admission_id IN(1)  AND
#                             -- (st.start_dttm >= '2024-02-01 00:00:00' AND st.start_dttm <= '2024-02-01 23:59:59')
#                             st.start_dttm >= {start_date} AND st.start_dttm <= {end_date}
#                             AND adm.org_id IN (select org_id from xocp_orgs where parent_id not in ('687','1872','2418') and org_id not in ('687','1872','2418'))
#                             AND adm.status_cd NOT IN ('cancelled','nullified') 
#                             ORDER BY adm.patient_id, adm.admission_id
#                         ) x
#                         GROUP BY x.PatientID, x.AdmissionID""" 
# source2 = pd.read_sql_query(query_source2,conn_ehr_live)
# print(source2)

# # source3 filter berdasarkan kolom updated_dttm
# query_source3 = f""" SELECT 
#                             x.PatientID,
#                             x.AdmissionID,
#                             x.MedicalNo,
#                             x.AdmissionDate,
#                             CASE 
#                                 WHEN GROUP_CONCAT(x.mark_stay) LIKE '%%y%%' THEN 'y'
#                                 ELSE 'n'
#                             END AS StayInd,
#                             x.stay_ind as StayIndAdmission
#                         FROM
#                         (
#                             SELECT
#                                 adm.patient_id as PatientID,
#                                 adm.admission_id as AdmissionID,
#                                 p.patient_ext_id as MedicalNo,
#                                 adm.admission_dttm as AdmissionDate,
#                                 st.in_org_id,
#                                 st.location_id,
#                                 b.sensus,
#                                 CASE 
#                                     WHEN b.sensus IS NOT NULL THEN 
#                                         CASE
#                                             WHEN st.in_org_id = 104 then 'n'
#                                             ELSE 'y'
#                                         END
#                                     ELSE 'n'
#                                 END AS mark_stay,
#                                 CASE
#                                     WHEN TIMEDIFF(st.stop_dttm, st.start_dttm) LIKE '%%-%%' THEN '00:00:00'
#                                     ELSE TIMEDIFF(st.stop_dttm, st.start_dttm)
#                                 END AS hours_of_stay,
#                                 st.start_dttm,
#                                 st.stop_dttm,
#                                 st.created_dttm,
#                                 st.updated_dttm,
#                                 adm.stay_ind
#                             FROM xocp_ehr_patient_admission adm 
#                             LEFT JOIN xocp_ehr_patient p on p.patient_id = adm.patient_id
#                             LEFT JOIN xocp_ehr_patient_stay st on adm.patient_id = st.patient_id and adm.admission_id = st.admission_id and st.status_cd NOT IN ('nullified','reserved') 
#                             LEFT JOIN xocp_ehr_location b on st.location_id = b.location_id AND st.in_org_id = b.org_id
#                             AND b.sensus = '1'
#                             WHERE 
#                             -- adm.patient_id IN (2077816) and adm.admission_id IN(1)  AND
#                             -- (st.updated_dttm >= '2024-02-01 00:00:00' AND st.updated_dttm <= '2024-02-01 23:59:59')
#                             st.updated_dttm >= {start_date} AND st.updated_dttm <={end_date}
#                             AND adm.org_id IN (select org_id from xocp_orgs where parent_id not in ('687','1872','2418') and org_id not in ('687','1872','2418'))
#                             AND adm.status_cd NOT IN ('cancelled','nullified') 
#                             ORDER BY adm.patient_id, adm.admission_id
#                         ) x
#                         GROUP BY x.PatientID, x.AdmissionID"""
# source3 = pd.read_sql_query(query_source3, conn_ehr_live)
# print(source3)

# source = pd.concat([source1,source2,source3], ignore_index=True)
# source = source.drop_duplicates(subset=['PatientID','AdmissionID'])
source = source1
source['Flag'] = '1'
print('source after filter')
# source=source[source['PatientID']== 616070]
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
print('cek target')
print(target)

# update 9 juli gajadi pake script di bawah ini karena tetep mau apa adanya dari EHR. 
# seandainya di DWH udah inap dan ehr berubah jadi n, maka di dwh bakal berubah jadi n
    
# untuk memisahkan yang StayInd dari target yang valuenya y, tetapi StayInd dari source berubah jadi n
stayind_mismatch = target[target['StayInd'] == 'y'].merge(source[source['StayInd'] == 'n'], how='inner', on=['PatientID', 'AdmissionID', 'MedicalNo', 'AdmissionDate'], suffixes=('', '_y'))

# Hapus columns dengan suffix '_y'
stayind_mismatch = stayind_mismatch[[col for col in stayind_mismatch.columns if not col.endswith('_y')]]

print('ini mismatch')
print(stayind_mismatch)

# Buat kondisi variabel stayind_mismatch ada isinya, maka akan running ulang query berdasarkan PatientID dan AdmissionID dari tabel admisi
if not stayind_mismatch.empty:
    patientid = tuple(stayind_mismatch['PatientID'])
    admissionid = tuple(stayind_mismatch['AdmissionID'])

    # bikin function remove comma jika ada variable yang isinya cuma 1, variable ini akan di pakai IN CLAUSE di query target
    def remove_comma(x):
        if len(x) == 1:
            return str(x).replace(',','')
        else:
            return x
    
    patientid = remove_comma(patientid)
    admissionid = remove_comma(admissionid)

    # ini baru update 15 juli 2024
    # query buat narik data dari target lalu filter berdasarkan primary key
    query_cek = f""" SELECT 
                            x.PatientID,
                            x.AdmissionID,
                            x.MedicalNo,
                            x.AdmissionDate,
                            CASE 
                                WHEN GROUP_CONCAT(x.mark_stay) LIKE '%%y%%' THEN 'y'
                                ELSE 'n'
                            END AS StayInd,
                            x.stay_ind as StayIndAdmission,
                            '1' Flag
                        FROM
                        (
                            SELECT
                                adm.patient_id as PatientID,
                                adm.admission_id as AdmissionID,
                                p.patient_ext_id as MedicalNo,
                                adm.admission_dttm as AdmissionDate,
                                st.in_org_id,
                                st.location_id,
                                b.sensus,
                                CASE 
                                    WHEN b.sensus IS NOT NULL THEN 
                                        CASE
                                            WHEN st.in_org_id = 104 then 'n'
                                            ELSE 'y'
                                        END
                                    ELSE 'n'
                                END AS mark_stay,
                                CASE
                                    WHEN TIMEDIFF(st.stop_dttm, st.start_dttm) LIKE '%%-%%' THEN '00:00:00'
                                    ELSE TIMEDIFF(st.stop_dttm, st.start_dttm)
                                END AS hours_of_stay,
                                st.start_dttm,
                                st.stop_dttm,
                                st.created_dttm,
                                st.updated_dttm,
                                adm.stay_ind
                            FROM xocp_ehr_patient_admission adm 
                            LEFT JOIN xocp_ehr_patient p on p.patient_id = adm.patient_id
                            LEFT JOIN xocp_ehr_patient_stay st on adm.patient_id = st.patient_id and adm.admission_id = st.admission_id and st.status_cd NOT IN ('nullified','reserved') 
                            LEFT JOIN xocp_ehr_location b on st.location_id = b.location_id AND st.in_org_id = b.org_id
                            AND b.sensus = '1'
                            WHERE 
                            -- adm.patient_id IN (760218,1460) and adm.admission_id IN (2)
                            -- (adm.admission_dttm >= '2024-02-01 00:00:00' AND adm.admission_dttm <= '2024-02-01 23:59:59')
                            adm.patient_id = %s and adm.admission_id = %s
                            
                            AND adm.org_id IN (select org_id from xocp_orgs where parent_id not in ('687','1872','2418') and org_id not in ('687','1872','2418'))
                            -- AND adm.status_cd NOT IN ('cancelled','nullified') 
                            ORDER BY adm.patient_id, adm.admission_id
                        ) x
                        GROUP BY x.PatientID, x.AdmissionID"""
    
    source_changes = pd.DataFrame(columns=['PatientID','AdmissionID','MedicalNo','AdmissionDate','StayInd','StayIndAdmission','Flag'])

    # looping untuk setiap rows di dataframe stayind_mismatch  (data2 yang berisi stayind yang tadinya y tapi berubah jadi n)
    for index, row in stayind_mismatch.iterrows():
        pk_values = (row['PatientID'],row['AdmissionID']) # ambil patient id dan admission id nya

        # Ensure pk_values is a tuple with the correct data types
        if not isinstance(pk_values, tuple):
            pk_values = tuple(pk_values)
        
        # jalankan query cek berdasarkan patientid dan admissionid dari stayind_mismatch
        results = conn_ehr.execute(query_cek,pk_values).fetchall()

        # jika results ada isinya, maka akan masuk ke dataframe source_changes
        if results:
            result_df = pd.DataFrame.from_records(results, columns=source_changes.columns)
            source_changes = pd.concat([source_changes, result_df], ignore_index=True)
        else:
            print('the results is empty')

    # deteksi perubahan pada variabel source_changes dengan target
    changes1 = source_changes[~source_changes.apply(tuple,1).isin(target.apply(tuple,1))]
    print('ini changes1')
    print(changes1)

    # filter PatientID dan AdmissionID yang tidak termasuk dalam stayind_mismatch
    mask = ~source.set_index(['PatientID', 'AdmissionID']).index.isin(stayind_mismatch.set_index(['PatientID', 'AdmissionID']).index)

    # filter source sesuai dengan PatientID dan AdmissionID yang didapat dari variabel mask
    source = source[mask]

    # deteksi perubahan pada variabel source dan target
    changes2 = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]
    print(changes2)

    changes = pd.concat([changes1,changes2],ignore_index=True)
    changes = changes.drop_duplicates(subset=['PatientID','AdmissionID'])
    
    print('changes gabungan')
    print(changes)
    
# jika tidak ada isinya maka filter yang bukan mismatch
else :
    # filter PatientID dan AdmissionID yang tidak termasuk dalam stayind_mismatch
    mask = ~source.set_index(['PatientID', 'AdmissionID']).index.isin(stayind_mismatch.set_index(['PatientID', 'AdmissionID']).index)

    # filter source sesuai dengan PatientID dan AdmissionID yang didapat dari variabel mask
    source = source[mask]

    # deteksi perubahan pada variabel source dan target
    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]
    print(changes)

# ambil data yang update
modified = changes[changes[['PatientID','AdmissionID']].apply(tuple,1).isin(target[['PatientID','AdmissionID']].apply(tuple,1))]
total_row_upd = len(modified)
text_upd = f'total row update : {total_row_upd}'
print(text_upd)
print(modified.iloc[:,:6])

# ambil data yang baru
inserted = changes[~changes[['PatientID','AdmissionID']].apply(tuple,1).isin(target[['PatientID','AdmissionID']].apply(tuple,1))]
total_row_ins = len(inserted)
text_ins = f'total row inserted : {total_row_ins}'
print(text_ins)
print(inserted.iloc[:,:6])

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
        update_stmt_5 = f'INNER JOIN (SELECT * from staging_rscm.{temp_table}) AS s ON t.{pk_1} = s.{pk_1} AND t.{pk_2} = s.{pk_2}  '
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

text= f'scheduler tanggal : {date}'
print(text)
sys.stdout.close()

db_connection.close_connection(conn_ehr)
db_connection.close_connection(conn_staging_sqlserver)

sys.stdout.close()