import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pyodbc
import pandas as pd
import time
import datetime as dt
date = dt.datetime.today()

# bikin log ke file
sys.stdout = open("C:/TestPython/DWH_SQL_Server/DWH/logs/LogFactPatientDrugPrescription.txt","w")
t0 = time.time()

# bikin koneksi ke db
conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)
conn_dwh_sqlserver = db_connection.create_connection(db_connection.dwh_sqlserver)

source = pd.read_sql_query(""" SELECT
                                    TRIM(a.PrescriptionID) AS PrescriptionID,
                                    a.SequenceID,
                                    a.PatientID,
                                    a.AdmissionID,
                                    a.PrescriptionOrgID,
                                    a.DrugID,
                                    a.DrugName,
                                    a.DrugQuantity,
                                    a.FrequencyCode,
                                    a.UnitCode,
                                    a.DispenseQuantity,
                                    a.DispenseUnitCode,
                                    a.IsFornas,
                                    a.CreatedID,
                                    a.DoctorID,
                                    a.DrugPrescriptionDate,
                                    a.PrescriptionNotes,
                                    a.PrescriptionStatus
                                FROM
                                staging_rscm.TransPatientDrugPrescription a
                                LEFT JOIN staging_rscm.DimensionPatientMPI b on a.PatientID = b.PatientID AND b.ScdActive = 1 
                                WHERE 
							    -- AND b.MedicalNo NOT IN (SELECT MedicalNo from staging_rscm.DimensionDummyPatient)
                                -- AND (DrugPrescriptionDate >= '2024-07-07 00:00:00' and DrugPrescriptionDate <= '2024-07-10 23:59:59')
                                -- AND ((InsertDateStaging >= '2024-05-02 00:00:00' and InsertDateStaging <= '2024-05-02 23:59:59')
                                -- OR (UpdateDateStaging >= '2024-05-02 00:00:00' and UpdateDateStaging <= '2024-05-02 23:59:59'))
                                ((CAST(InsertDateStaging as date) >= CAST(DATEADD(DAY, -1,GETDATE()) as date) AND CAST(InsertDateStaging as date) <= CAST(GETDATE() as date))
                                OR (CAST(UpdateDateStaging as date) >= CAST(DATEADD(DAY, -1,GETDATE()) as date) AND CAST(UpdateDateStaging as date) <= CAST(GETDATE() as date)))
                                AND (MedicalNo NOT IN (SELECT MedicalNo FROM staging_rscm.DimensionDummyPatient) OR MedicalNo IS NULL)
                                -- AND a.PrescriptionID IN ('00140001399050','00140001399584')     
                                -- AND CAST(UpdateDateStaging as date) = CAST(GETDATE() as date)
                                """, conn_staging_sqlserver)

print(source.iloc[:,0:8])

if source.empty:
    print('tidak ada data dari source')
else:
    # jika dari source cuma 1 row
    if len(source) == 1:        
        # ambil primary key dari source, ambil index ke 0
        prescriptionid = source["PrescriptionID"].values[0]
        sequenceid = source["SequenceID"].values[0]

        # query buat narik data dari target lalu filter berdasarkan primary key
        query = f'SELECT TRIM(PrescriptionID) as PrescriptionID,SequenceID,PatientID,AdmissionID,PrescriptionOrgID,DrugID,DrugName,DrugQuantity,FrequencyCode,UnitCode,DispenseQuantity,DispenseUnitCode,IsFornas,CreatedID,DoctorID,DrugPrescriptionDate,PrescriptionNotes,PrescriptionStatus from dwhrscm_talend.FactPatientDrugPrescription where PrescriptionID IN ({prescriptionid}) AND SequenceID IN ({sequenceid}) order by PrescriptionID,SequenceID'
        target = pd.read_sql_query(query, conn_dwh_sqlserver)
    else :
         # ambil primary key dari source, pake unique biar tidak duplicate
        prescriptionid = tuple(source["PrescriptionID"].unique())
        sequenceid = tuple(source["SequenceID"].unique())

         # query buat narik data dari target lalu filter berdasarkan primary key
        query = f'SELECT TRIM(PrescriptionID) as PrescriptionID,SequenceID,PatientID,AdmissionID,PrescriptionOrgID,DrugID,DrugName,DrugQuantity,FrequencyCode,UnitCode,DispenseQuantity,DispenseUnitCode,IsFornas,CreatedID,DoctorID,DrugPrescriptionDate,PrescriptionNotes,PrescriptionStatus from dwhrscm_talend.FactPatientDrugPrescription where PrescriptionID IN {prescriptionid} AND SequenceID IN {sequenceid} order by PrescriptionID,SequenceID'
        target = pd.read_sql_query(query, conn_dwh_sqlserver)
    print('\nIni Target\n')
    print(target.iloc[:,0:8])
    print('\n')


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

    if modified.empty & inserted.empty :
        print('\ntidak ada data yang bisa diproses karena data yang berubah dan yang baru kosong')

    elif modified.empty:
        # bikin tanggal sekarang buat kolom InsertedDateDWH
        today = dt.datetime.now()
        today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
        inserted['InsertedDateDWH'] = today_convert
        inserted.to_sql('FactPatientDrugPrescription', schema='dwhrscm_talend', con=conn_dwh_sqlserver, if_exists = 'append', index=False)
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
            df.to_sql(temp_table, schema= 'dwhrscm_talend',con=conn_dwh_sqlserver, if_exists='replace',index=False)
            update_stmt_1 = f'UPDATE r '
            update_stmt_2 = f'SET '
            update_stmt_3 = ", ".join(list_col)
            update_stmt_8 = f' , r.UpdateDateDWH = CONVERT(DATETIME2(0), GETDATE(),120)'
            update_stmt_4 = f' FROM dwhrscm_talend.{table} r '
            update_stmt_5 = f'INNER JOIN (SELECT * FROM dwhrscm_talend.{temp_table}) as t ON r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} '  
            update_stmt_6 = f' WHERE r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} '
            update_stmt_7 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_8 + update_stmt_4 + update_stmt_5 + update_stmt_6 +";"
            delete_stmt_1 = f'DROP TABLE dwhrscm_talend.{temp_table}'
            print(update_stmt_7)
            conn_dwh_sqlserver.execute(update_stmt_7)
            conn_dwh_sqlserver.execute(delete_stmt_1)

        try:
            # update data
            updated_to_sql(modified, 'FactPatientDrugPrescription', 'PrescriptionID', 'SequenceID')

            #insert data baru
            today = dt.datetime.now()
            today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
            inserted['InsertedDateDWH'] = today_convert
            inserted.to_sql('FactPatientDrugPrescription', schema='dwhrscm_talend',con=conn_dwh_sqlserver, if_exists ='append',index=False)
            print('success update and insert all data')
        
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