import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pandas as pd
import pyodbc
import datetime as dt
date = dt.datetime.today()
import time

# bikin log ke file
sys.stdout = open("C:/TestPython/DWH_SQL_Server/DWH/logs/LogFactPatientStay.txt","w")
t0 = time.time()

conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)
conn_dwh_sqlserver = db_connection.create_connection(db_connection.dwh_sqlserver)

# tarik query, masuk ke variabel source
source = pd.read_sql_query(""" SELECT
                                    PatientID,
                                    AdmissionID,
                                    MedicalNo,
                                    AdmissionDate,
                                    StayInd,
                                    StayIndAdmission,
                                    Flag
                                FROM staging_rscm.TransPatientStay
                                WHERE 
                                -- CAST(AdmissionDate as date) >= '2025-02-10' AND CAST(AdmissionDate as date) <= '2025-02-15'
                                (CAST(InsertDateStaging as date) >= CAST(DATEADD(DAY, -2, GETDATE()) as date) AND CAST(InsertDateStaging as date) <= CAST(GETDATE() as date)
                                OR 
                                CAST(UpdateDateStaging as date) >= CAST(DATEADD(DAY, -2, GETDATE()) as date) AND CAST(UpdateDateStaging as date) <= CAST(GETDATE() as date))
                                -- AND PatientID = 2077831 and AdmissionID = 1     
                                -- (CAST(InsertDateStaging as date) >= CAST(GETDATE() as date) AND CAST(InsertDateStaging as date) <= CAST(GETDATE() as date)
                                -- OR 
                                -- CAST(UpdateDateStaging as date) >= CAST(GETDATE() as date) AND CAST(UpdateDateStaging as date) <= CAST(GETDATE() as date))
                                AND (MedicalNo NOT IN (SELECT MedicalNo FROM staging_rscm.DimensionDummyPatient) OR MedicalNo IS NULL)
                                -- AND PatientID = 96934
                                """, conn_staging_sqlserver)

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
query = f'SELECT PatientID,AdmissionID,MedicalNo,AdmissionDate,StayInd,StayIndAdmission,Flag from dwhrscm_talend.FactPatientStay where PatientID IN {patientid} AND AdmissionID IN {admissionid} order by PatientID, AdmissionID'
target = pd.read_sql_query(query, conn_dwh_sqlserver)
print(target)
# ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

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
    today = dt.datetime.now()
    today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
    inserted['InsertedDateDWH'] = today_convert
    inserted.to_sql('FactPatientStay', schema='dwhrscm_talend', con = conn_dwh_sqlserver, if_exists='append',index=False)
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
        df.to_sql(temp_table, schema = 'dwhrscm_talend', con = conn_dwh_sqlserver, if_exists='replace',index = False)
        update_stmt_1 = f'UPDATE t '
        update_stmt_2 = f'SET '
        update_stmt_3 = ", ".join(list_col)
        update_stmt_8 = f' , t.UpdateDateDWH = CONVERT(DATETIME2(0), GETDATE(),120)'
        update_stmt_4 = f' FROM dwhrscm_talend.{table} t '
        update_stmt_5 = f'INNER JOIN (SELECT * from dwhrscm_talend.{temp_table}) AS s ON t.{pk_1} = s.{pk_1} AND t.{pk_2} = s.{pk_2} '
        update_stmt_6 = f'WHERE t.{pk_1} = s.{pk_1} AND t.{pk_2} = s.{pk_2} '
        update_stmt_7 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_8 + update_stmt_4 + update_stmt_5 +  update_stmt_6 +";"
        delete_stmt_1 = f'DROP TABLE dwhrscm_talend.{temp_table}'
        print(update_stmt_7)
        print('\n')
        conn_dwh_sqlserver.execute(update_stmt_7)
        conn_dwh_sqlserver.execute(delete_stmt_1)

    try:
        # call fungsi update
        updated_to_sql(modified, 'FactPatientStay', 'PatientID','AdmissionID')
        # masukkan data yang baru ke table target
        today = dt.datetime.now()
        today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
        inserted['InsertedDateDWH'] = today_convert
        inserted.to_sql('FactPatientStay', schema='dwhrscm_talend', con = conn_dwh_sqlserver, if_exists='append',index=False)
        print('success update dan insert')
    except Exception as e:
        print(e)

t1 = time.time()
total=t1-t0
print(total)

text=f'scheduler tanggal : {date}'
print(text)

db_connection.close_connection(conn_staging_sqlserver)
db_connection.close_connection(conn_dwh_sqlserver)
sys.stdout.close()