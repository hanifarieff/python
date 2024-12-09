import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pyodbc
import pandas as pd
import time
import datetime as dt
date = dt.datetime.today()

# bikin log ke file
sys.stdout = open("C:/TestPython/DWH_SQL_Server/DWH/logs/LogFactPatientBMI.txt","w")
t0 = time.time()

conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)
conn_dwh_sqlserver = db_connection.create_connection(db_connection.dwh_sqlserver)

source = pd.read_sql_query("""
                           SELECT
                                a.OrderObsID,
                                a.PatientID,
                                a.AdmissionID,
                                a.ObservationDate,
                                a.PanelName,
                                a.BMI,
                                a.BMIStatus,
                                a.BMIDetail,
                                a.BMIDetailStatus
                            FROM staging_rscm.TransPatientBMI a 
                            LEFT JOIN staging_rscm.DimensionPatientMPI b on a.PatientID = b.PatientID and b.ScdActive = 1
                            WHERE b.MedicalNo NOT IN (SELECT MedicalNo FROM staging_rscm.DimensionDummyPatient)
                            AND (CAST(a.InsertDateStaging as date) >= CAST(DATEADD(DAY, -2, GETDATE()) as date) AND CAST(a.InsertDateStaging as date) <= CAST(GETDATE() as date) 
                            OR CAST(a.UpdateDateStaging as date) >= CAST(DATEADD(DAY, -2, GETDATE()) as date) AND CAST(a.UpdateDateStaging as date) <= CAST(GETDATE() as date))                                             
                         """,con=conn_staging_sqlserver)

print(source)

if source.empty:
    print('tidak ada data dari source')
else:
    # jika dari source cuma 1 row
    if len(source) == 1:        
        OrderObsID = source["OrderObsID"].values[0]     

        # query buat narik data dari target lalu filter berdasarkan primary key
        query = f'SELECT OrderObsID, PatientID, AdmissionID, ObservationDate, PanelName, BMI,BMIStatus, BMIDetail,BMIDetailStatus FROM dwhrscm_talend.FactPatientBMI WHERE OrderObsID IN ({OrderObsID})'
        target = pd.read_sql_query(query, conn_dwh_sqlserver)
    else :
         # ambil primary key dari source, pake unique biar tidak duplicate
        OrderObsID = tuple(source["OrderObsID"].unique())

         # query buat narik data dari target lalu filter berdasarkan primary key
        query = f'SELECT OrderObsID, PatientID, AdmissionID, ObservationDate, PanelName, BMI,BMIStatus, BMIDetail,BMIDetailStatus FROM dwhrscm_talend.FactPatientBMI WHERE OrderObsID IN {OrderObsID}'
        target = pd.read_sql_query(query, conn_dwh_sqlserver)
    
    # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

    # ambil data yang update
    modified = changes[changes[['OrderObsID']].apply(tuple,1).isin(target[['OrderObsID']].apply(tuple,1))]
    total_row_upd = len(modified)
    text_upd = f'total row update : {total_row_upd}'
    print(text_upd)
    print(modified)

    #ambil data yang baru
    inserted = changes[~changes[['OrderObsID']].apply(tuple,1).isin(target[['OrderObsID']].apply(tuple,1))]
    total_row_ins = len(inserted)
    text_ins = f'total row inserted : {total_row_ins}'
    print(text_ins)
    print(inserted)

    if modified.empty:
        if inserted.empty :
            print('tidak ada data yang baru dan berubah')
        else:
            inserted.to_sql('FactPatientBMI', schema='dwhrscm_talend' ,con=conn_dwh_sqlserver, if_exists = 'append', index=False)
            print('success insert without update')
    else:
        # buat fungsi update data
        def updated_data(df, table_name, key_1):
            a = []
            table = table_name
            pk_1 = key_1
            temp_table = f'{table}_temporary_table'
            for col in df.columns:
                if col == pk_1 :
                    continue
                a.append(f't.{col} = s.{col}')
            df.to_sql(temp_table, schema='dwhrscm_talend', con=conn_dwh_sqlserver, if_exists = 'replace', index = False)
            update_stmt_1 = f'UPDATE t '
            update_stmt_2 = f'SET '
            update_stmt_3 = ", ".join(a)
            update_stmt_8 = f' , t.UpdateDateDWH = CONVERT(DATETIME2(0), GETDATE(),120)'
            update_stmt_4 = f' FROM dwhrscm_talend.{table} t '
            update_stmt_5 = f'INNER JOIN (SELECT * FROM dwhrscm_talend.{temp_table}) AS s ON t.{pk_1} = s.{pk_1}  '
            update_stmt_6 = f' WHERE t.{pk_1} = s.{pk_1} '
            update_stmt_7 = update_stmt_1 +  update_stmt_2 + update_stmt_3 + update_stmt_8 + update_stmt_4 + update_stmt_5 + update_stmt_6 +";"
            delete_stmt_1 = f'DROP TABLE dwhrscm_talend.{temp_table} '
            print(update_stmt_7)
            conn_dwh_sqlserver.execute(update_stmt_7)
            conn_dwh_sqlserver.execute(delete_stmt_1)

        try:
            #update data
            updated_data(modified, 'FactPatientBMI', 'OrderObsID')

            #insert data
            inserted.to_sql('FactPatientBMI', schema='dwhrscm_talend', con=conn_dwh_sqlserver, if_exists = 'append', index=False)

            print('all success updated and inserted')
        except Exception as e:
            print(e)

#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)

text=f'scheduler tanggal : {date}'
print(text)

db_connection.close_connection(conn_staging_sqlserver)
db_connection.close_connection(conn_dwh_sqlserver)