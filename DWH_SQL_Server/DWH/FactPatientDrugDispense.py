import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pyodbc
import pandas as pd
import time
import datetime as dt
date = dt.datetime.today()

# bikin log ke file
sys.stdout = open("C:/TestPython/DWH_SQL_Server/DWH/logs/LogFactPatientDrugDispense.txt","w")
t0 = time.time()

# bikin koneksi ke db
conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)
conn_dwh_sqlserver = db_connection.create_connection(db_connection.dwh_sqlserver)

source = pd.read_sql_query(""" SELECT
                                    PrescriptionID,
                                    OrderID,
                                    SequenceID,
                                    PatientID,
                                    AdmissionID,
                                    MedicalNo,
                                    DrugID,
                                    DrugName,
                                    DrugQuantity,
                                    DrugUnitPrice,
                                    DrugTotalPrice,
                                    UnitCode,
                                    DispenseStatus,
                                    DispenseMethod,
                                    IsFornas,
                                    CreatedID,
                                    DoctorID,
                                    DrugPrescriptionDate,
                                    DrugDispenseDate
                                FROM staging_rscm.TransPatientDrugDispense
                                WHERE 
                                -- PrescriptionID = '00150000491630'
                                -- UpdateDateStaging >= '2024-03-07 04:00:00' and UpdateDateStaging <= '2024-03-07 04:10:59'
                                -- DrugDispenseDate >= '2024-10-01 00:00:00' and DrugDispenseDate <= '2024-10-01 23:59:59'
                                CAST(InsertDateStaging as date) >= CAST(DATEADD(DAY, -1, GETDATE()) as date) AND CAST(InsertDateStaging as date) <= CAST(GETDATE() as date)
                                OR (CAST(UpdateDateStaging as date) >= CAST(DATEADD(DAY, -1, GETDATE()) as date) AND CAST(UpdateDateStaging as date) <= CAST(GETDATE() as date))
                                AND (MedicalNo NOT IN (SELECT MedicalNo FROM staging_rscm.DimensionDummyPatient) OR MedicalNo IS NULL)
                           """, conn_staging_sqlserver)
source
print(source)

if source.empty:
    print('tidak ada data dari source')
else:
    prescriptionid = tuple(source["PrescriptionID"].unique())
    orderid = tuple(source["OrderID"].unique())
    sequenceid = tuple(source["SequenceID"].unique())

    if len(prescriptionid) > 1:
        pass
    else:
        prescriptionid = str(prescriptionid).replace(',','')

    if len(orderid) > 1:
        pass
    else:
        orderid = str(orderid).replace(',','')
    
    if len(sequenceid) > 1:
        pass
    else:
        sequenceid = str(sequenceid).replace(',','')

     # query buat narik data dari target lalu filter berdasarkan primary key
    query = f'SELECT PrescriptionID,OrderID,SequenceID,PatientID,AdmissionID,MedicalNo,DrugID,DrugName,DrugQuantity,DrugUnitPrice,DrugTotalPrice,UnitCode,DispenseStatus,DispenseMethod,IsFornas,CreatedID,DoctorID,DrugPrescriptionDate,DrugDispenseDate from dwhrscm_talend.FactPatientDrugDispense where PrescriptionID IN {prescriptionid} AND OrderID IN {orderid} AND SequenceID IN {sequenceid} order by PrescriptionID,OrderID,SequenceID'
    target = pd.read_sql_query(query, conn_dwh_sqlserver)

    # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

    # ambil data yang update dari changes
    modified = changes[changes[['PrescriptionID','OrderID','SequenceID']].apply(tuple,1).isin(target[['PrescriptionID','OrderID','SequenceID']].apply(tuple,1))]
    total_row_upd = len(modified)
    text_upd = f'total row update : {total_row_upd}'
    print(text_upd)
    print(modified)

    # ambil data yang new dari changes
    inserted = changes[~changes[['PrescriptionID','OrderID','SequenceID']].apply(tuple,1).isin(target[['PrescriptionID','OrderID','SequenceID']].apply(tuple,1))]
    total_row_ins = len(inserted)
    text_ins = f'total row inserted : {total_row_ins}'
    print(text_ins)
    print(inserted)

    if modified.empty:
        # bikin tanggal sekarang buat kolom InsertedDateDWH
        today = dt.datetime.now()
        today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
        inserted['InsertedDateDWH'] = today_convert
        inserted.to_sql('FactPatientDrugDispense', schema='dwhrscm_talend', con = conn_dwh_sqlserver, if_exists = 'append', index=False)
        print('success insert all data without update')
    
    else:
        # buat fungsi untuk update data ke tabel target
        def updated_to_sql(df, table_name, key_1,key_2,key_3):
            list_col = []
            table=table_name
            pk_1 = key_1
            pk_2 = key_2
            pk_3 = key_3
            temp_table = f'{table}_temporary_table'
            for col in df.columns:
                if col == pk_1 or col == pk_2 or col == pk_3:
                    continue
                list_col.append(f'r.{col} = t.{col}')
            df.to_sql(temp_table,schema = 'dwhrscm_talend',con=conn_dwh_sqlserver, if_exists='replace',index=False)
            update_stmt_1 = f'UPDATE r '
            update_stmt_2 = f'SET '
            update_stmt_3 = ", ".join(list_col)
            update_stmt_8 = f' , r.UpdateDateDWH = CONVERT(DATETIME2(0), GETDATE(),120)'
            update_stmt_4 = f' FROM dwhrscm_talend.{table} r '
            update_stmt_5 = f'INNER JOIN (SELECT * FROM dwhrscm_talend.{temp_table}) as t ON r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} AND r.{pk_3} = t.{pk_3} '
            update_stmt_6 = f'WHERE r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} AND r.{pk_3} = t.{pk_3} '
            update_stmt_7 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_8 + update_stmt_4 + update_stmt_5 + update_stmt_6 +";"
            delete_stmt_1 = f'DROP TABLE dwhrscm_talend.{temp_table}'
            print(update_stmt_7)
            conn_dwh_sqlserver.execute(update_stmt_7)
            conn_dwh_sqlserver.execute(delete_stmt_1)

        try:
            # update data
            updated_to_sql(modified, 'FactPatientDrugDispense', 'PrescriptionID', 'OrderID', 'SequenceID')

            # insert data baru
            today = dt.datetime.now()
            today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
            inserted['InsertedDateDWH'] = today_convert
            inserted.to_sql('FactPatientDrugDispense', schema='dwhrscm_talend', con=conn_dwh_sqlserver, if_exists ='append',index=False)
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