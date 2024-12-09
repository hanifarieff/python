import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pandas as pd
import pyodbc
import numpy as np
import datetime as dt
date = dt.datetime.today()
import time
import sys

sys.stdout = open("C:/TestPython/DWH_SQL_Server/DWH/logs/LogFactPathologyAnatomy.txt","w")

t0 = time.time()

conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)
conn_dwh_sqlserver = db_connection.create_connection(db_connection.dwh_sqlserver)

source = pd.read_sql_query("""                                  
                            SELECT
                                TRIM(a.RegistrationNo) AS RegistrationNo,
                                a.OrderID,
                                a.PatientID,
                                a.AdmissionID,
                                b.MedicalNo,
                                a.PanelID,
                                a.PanelName,
                                a.ObjID,
                                a.ObjName,
                                a.ObsValue,
                                CASE
                                    WHEN a.ObjID = 68725 AND a.ObsValue LIKE '%/%' THEN SUBSTRING(a.ObsValue,CHARINDEX('/',a.ObsValue)+1,LEN(a.ObsValue))
                                    ELSE '-'
                                END AS Behaviour,
                                a.Status,
                                a.CreatedUserID,
                                a.CreatedDate,
                                a.FinalUserID,
                                a.FinalDate
                            FROM staging_rscm.TransPathologyAnatomy a
                            LEFT JOIN DWH_RSCM.dwhrscm_talend.DimPatientMPI b on a.PatientID = b.PatientID and b.ScdActive = '1'
                            
                            WHERE 
                            (CAST(a.InsertDateStaging as date) >= CAST(DATEADD(DAY, -2, GETDATE()) as date) AND CAST(a.InsertDateStaging as date) <= CAST(GETDATE() as date) 
                            OR CAST(a.UpdateDateStaging as date) >= CAST(DATEADD(DAY, -2, GETDATE()) as date) AND CAST(a.UpdateDateStaging as date) <= CAST(GETDATE() as date)) 
                            -- a.CreatedDate >= '2024-07-31 00:00:00' and a.CreatedDate <= '2024-07-31 23:59:59'
                            AND (MedicalNo NOT IN (SELECT MedicalNo FROM staging_rscm.DimensionDummyPatient) OR MedicalNo IS NULL)
                    """, conn_staging_sqlserver)

print(source)

# Apply regex only for ObjID == 68724
source['TopoCode'] = source['ObsValue'].str.extract(r'(C\d+\.?\d*)')

# For ObjID != 68724, assign a dash ('-')
source['TopoCode'] = np.where(source['ObjID'] == 68724, source['TopoCode'], '-')

print(source[source['ObjID']== 68724])
# source['ObsValue'] = source['ObsValue'].replace('\t', ' ').replace('\\n','\n')
# source['FinalUserID'] = source['FinalUserID'].replace(np.nan,0).astype('int64')

if source.empty:
    print('tidak ada data dari source')
else:
    # ambil primary key dari source, pake unique biar tidak duplicate
    registration_no = tuple(source["RegistrationNo"].unique())
    order_id = tuple(source["OrderID"].unique())
    patient_id = tuple(source["PatientID"].unique())
    admission_id = tuple(source["AdmissionID"].unique())
    obj_id = tuple(source["ObjID"].unique())

    if len(registration_no) > 1 :
        pass
    else :
        registration_no = str(registration_no).replace(',','')

    if len(order_id) > 1 :
        pass
    else :
        order_id = str(order_id).replace(',','')

    if len(patient_id) > 1 :
        pass
    else :
        patient_id = str(patient_id).replace(',','')

    if len(admission_id) > 1 :
        pass
    else :
        admission_id = str(admission_id).replace(',','')

    if len(obj_id) > 1 :
        pass
    else :
        obj_id = str(obj_id).replace(',','')
    # query buat narik data dari target lalu filter berdasarkan primary key
    query = f"""SELECT 
                    TRIM(RegistrationNo) as RegistrationNo, 
                    OrderID, 
                    PatientID, 
                    AdmissionID, 
                    MedicalNo,
                    PanelID,
                    PanelName, 
                    ObjID,
                    ObjName, 
                    ObsValue,
                    Behaviour,
                    Status,
                    CreatedUserID,
                    CreatedDate,
                    FinalUserID,
                    FinalDate,
                    TopoCode 
                FROM dwhrscm_talend.FactPathologyAnatomy WHERE RegistrationNo IN {registration_no} AND 
                OrderID IN {order_id} AND PatientID IN {patient_id} AND AdmissionID IN {admission_id} AND ObjID IN {obj_id}
            """
    target = pd.read_sql_query(query, conn_dwh_sqlserver)
    print(target)

    # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

    # ambil data yang update
    modified = changes[changes[['RegistrationNo','OrderID','PatientID','AdmissionID','ObjID']].apply(tuple,1).isin(target[['RegistrationNo','OrderID','PatientID','AdmissionID','ObjID']].apply(tuple,1))]
    total_row_upd = len(modified)
    text_upd = f'total row update : {total_row_upd}'
    print(text_upd)
    print(modified)

    #ambil data yang baru
    inserted = changes[~changes[['RegistrationNo','OrderID','PatientID','AdmissionID','ObjID']].apply(tuple,1).isin(target[['RegistrationNo','OrderID','PatientID','AdmissionID','ObjID']].apply(tuple,1))]
    total_row_ins = len(inserted)
    text_ins = f'total row inserted : {total_row_ins}'
    print(text_ins)
    print(inserted)

    if modified.empty:
        if inserted.empty :
            print('tidak ada data yang baru dan berubah')
        else:
            inserted.to_sql('FactPathologyAnatomy', schema='dwhrscm_talend' ,con=conn_dwh_sqlserver, if_exists = 'append', index=False)
            print('success insert without update')
    else:
        # buat fungsi update data
        def updated_data(df, table_name, key_1,key_2,key_3,key_4,key_5):
            a = []
            table = table_name
            pk_1 = key_1
            pk_2 = key_2
            pk_3 = key_3
            pk_4 = key_4
            pk_5 = key_5
            temp_table = f'{table}_temporary_table'
            for col in df.columns:
                if col == pk_1 or col == pk_2 or col == pk_3 or col == pk_4 or col == pk_5 :
                    continue
                a.append(f't.{col} = s.{col}')
            df.to_sql(temp_table, schema='dwhrscm_talend', con=conn_dwh_sqlserver, if_exists = 'replace', index = False)
            update_stmt_1 = f'UPDATE t '
            update_stmt_2 = f'SET '
            update_stmt_3 = ", ".join(a)
            update_stmt_8 = f' , t.UpdateDateDWH = CONVERT(DATETIME2(0), GETDATE(),120)'
            update_stmt_4 = f' FROM dwhrscm_talend.{table} t '
            update_stmt_5 = f'INNER JOIN (SELECT * FROM dwhrscm_talend.{temp_table}) AS s ON t.{pk_1} = s.{pk_1} AND t.{pk_2} = s.{pk_2} AND t.{pk_3} = s.{pk_3} AND t.{pk_4} = s.{pk_4} AND t.{pk_5} = s.{pk_5} '
            update_stmt_6 = f' WHERE t.{pk_1} = s.{pk_1}  AND t.{pk_2} = s.{pk_2} AND t.{pk_3} = s.{pk_3} AND t.{pk_4} = s.{pk_4} AND t.{pk_5} = s.{pk_5} '
            update_stmt_7 = update_stmt_1 +  update_stmt_2 + update_stmt_3 + update_stmt_8 + update_stmt_4 + update_stmt_5 + update_stmt_6 +";"
            delete_stmt_1 = f'DROP TABLE dwhrscm_talend.{temp_table} '
            print(update_stmt_7)
            conn_dwh_sqlserver.execute(update_stmt_7)
            conn_dwh_sqlserver.execute(delete_stmt_1)

        try:
            #update data
            updated_data(modified, 'FactPathologyAnatomy', 'RegistrationNo','OrderID','PatientID','AdmissionID','ObjID')

            #insert data
            inserted.to_sql('FactPathologyAnatomy', schema='dwhrscm_talend', con=conn_dwh_sqlserver, if_exists = 'append', index=False)

            print('all success updated and inserted')
        except Exception as e:
            print(e)

#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)
print('\n')
text=f'scheduler tanggal : {date}'
print(text)

db_connection.close_connection(conn_staging_sqlserver)
db_connection.close_connection(conn_dwh_sqlserver)
sys.stdout.close()


