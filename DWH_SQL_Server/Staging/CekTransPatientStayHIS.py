from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine
import pandas as pd
import pyodbc
import datetime as dt
date = dt.datetime.today()
import time
import sys

sys.stdout = open("C:/TestPython/DWH_SQL_Server/Staging/logs/LogCekTransStayIndHIS.txt","w")
t0 = time.time()

his =  create_engine('mysql://hanif-ppi:hanif2022@172.16.19.21/his')
dwh_sql_server = create_engine('mssql+pyodbc://andhi-ppi:Andhi2022!@172.16.19.36/StagingRSCM?driver=SQL+Server+Native+Client+11.0', fast_executemany=True)
try :
    conn_his = his.connect()
    conn_dwh_sqlserver = dwh_sql_server.connect()
    print('successfully connect to HIS & DWH SQL Server database')
except SQLAlchemyError as e:
    error = str(e.__dict__['orig'])
    print(error)

# tarik query, masuk ke variabel source
source = pd.read_sql_query(""" select 
                                a.PatientID as PatientID,
                                a.AdmissionID as AdmissionID,
                                a.AdmissionDate as AdmissionDate,
                                b.PatientID as PatientIDNull
                            from staging_rscm.TransPatientOrder a 
                            left join staging_rscm.TransPatientStay b on a.PatientID = b.PatientID and a.AdmissionID = b.AdmissionID
                            where b.PatientID is null
                            order by a.PatientID, a.AdmissionID """, conn_dwh_sqlserver)

# ambil key dari source
patientidold = tuple(source["PatientID"].unique()) 
admissionidold = tuple(source["AdmissionID"].unique())
if len(admissionidold) == 1:
    admissionidold=admissionidold[0]
else:
    pass

# cek StayInd berdasarkan patientid dan admissionid
query_cek = (
            f"SELECT DISTINCT "
                f"adm.patient_id as PatientID, "
                f"adm.admission_id as AdmissionID, "
                f"p.patient_mrn_txt as MedicalNo, "
                f"adm.admission_dttm as AdmissionDate, "
                f"CASE " 
                    f"WHEN c.patient_id IS NULL then 'n' "
                    f"ELSE c.StayInd "
                f"END AS StayInd "
            f"FROM xocp_his_patient_admission adm "
            f"LEFT JOIN xocp_his_patient p ON adm.patient_id = p.patient_id "
            f"LEFT JOIN "
                f"(SELECT "
                    f"b.patient_id, "
                    f"b.admission_id, "
                    f"b.created_dttm, "
                    f"CASE " 
                        f"WHEN b.StayInd LIKE '%%y%%' THEN 'y' "
                        f"ELSE 'n' "
                        f"END AS StayInd "
                f"FROM "
                    f"(SELECT " 
                        f"pl.patient_id, " 
                        f"pl.admission_id, "
                        f"GROUP_CONCAT(pl.StayInd ORDER BY pl.StayInd) as StayInd, "
                        f"MAX(pl.created_dttm) as created_dttm "
                    f"FROM  "
                        f"(SELECT " 
                            f"patient_id, "
                            f"admission_id,created_dttm,"
                            f"CASE "
                            f"WHEN location_id IN (206,207) THEN 'n' "
                            f"ELSE 'y' "
                            f"END AS StayInd "
                            f"FROM xocp_his_patient_location WHERE status_cd NOT IN ('nullified') "
                            f"ORDER BY patient_id,admission_id "
                        f") pl "
                    f"GROUP BY pl.patient_id, pl.admission_id "
                    f") b "
                f") c "
                f"ON adm.patient_id = c.patient_id and adm.admission_id = c.admission_id "
                f"WHERE adm.patient_id IN {patientidold} AND adm.admission_id IN {admissionidold} "
                f"AND adm.org_id in (select org_id from xocp_orgs where parent_id in ('687','1872') OR org_id in ('687','1872')) "
                f"ORDER BY adm.patient_id, adm.admission_id "
            )

source = pd.read_sql_query(query_cek, conn_his)

source['Flag'] = 2
print(source)

# query buat narik data dari target lalu filter berdasarkan primary key
if source.empty:
    print('tidak ada row yang bisa di proses karena source kosong')
else:
    # pake if jika dari source cuma ada 1 row
    if len(source) == 1:
        patientid = source["PatientID"].values[0]
        admissionid = source["AdmissionID"].values[0]
        query = f'SELECT PatientID,AdmissionID,MedicalNo,AdmissionDate,StayInd from staging_rscm.TransPatientStay where PatientID IN ({patientid}) AND AdmissionID IN ({admissionid}) AND Flag = 2 order by PatientID, AdmissionID'
        target = pd.read_sql_query(query, conn_dwh_sqlserver)
    else:
        patientid = tuple(source["PatientID"].unique()) 
        admissionid = tuple(source["AdmissionID"].unique())
        if len(admissionid) ==1:
            admissionid = source["AdmissionID"].values[0]
            query = f'SELECT PatientID,AdmissionID,MedicalNo,AdmissionDate,StayInd from staging_rscm.TransPatientStay where PatientID IN {patientid} AND AdmissionID IN ({admissionid}) AND Flag = 2 order by PatientID, AdmissionID'
        else :
            query = f'SELECT PatientID,AdmissionID,MedicalNo,AdmissionDate,StayInd from staging_rscm.TransPatientStay where PatientID IN {patientid} AND AdmissionID IN {admissionid} AND Flag = 2 order by PatientID, AdmissionID'
        target = pd.read_sql_query(query, conn_dwh_sqlserver)

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
        inserted.to_sql('TransPatientStay', schema='staging_rscm', con = conn_dwh_sqlserver, if_exists='append',index=False)
        print('success insert without update')
    else:
        # buat fungsi untuk update
        def updated_to_sql(df, table_name, key_1, key_2, key_3):
            list_col = []
            table = table_name
            pk_1 = key_1
            pk_2 = key_2
            pk_3 = key_3
            temp_table = f'{table}_temporary_table'
            for col in df.columns:
                if col == pk_1 or col == pk_2 or col == pk_3:
                    continue
                list_col.append(f't.{col} = s.{col}')
            df.to_sql(temp_table, schema = 'staging_rscm', con = conn_dwh_sqlserver, if_exists='replace',index = False)
            update_stmt_1 = f'UPDATE t '
            update_stmt_2 = f'SET '
            update_stmt_3 = ", ".join(list_col)
            update_stmt_4 = f' FROM staging_rscm.{table} t '
            update_stmt_5 = f'INNER JOIN (SELECT * from staging_rscm.{temp_table}) AS s ON t.{pk_1} = s.{pk_1} AND t.{pk_2} = s.{pk_2} AND t.{pk_3} = s.{pk_3} '
            update_stmt_6 = f'WHERE t.{pk_1} = s.{pk_1} AND t.{pk_2} = s.{pk_2} AND t.{pk_3} = s.{pk_3}'
            update_stmt_7 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_4 + update_stmt_5 +  update_stmt_6 +";"
            delete_stmt_1 = f'DROP TABLE staging_rscm.{temp_table}'
            print(update_stmt_7)
            print('\n')
            conn_dwh_sqlserver.execute(update_stmt_7)
            conn_dwh_sqlserver.execute(delete_stmt_1)

        try:
            # call fungsi update
            updated_to_sql(modified, 'TransPatientStay', 'PatientID','AdmissionID','Flag')
            # masukkan data yang baru ke table target
            inserted.to_sql('TransPatientStay', schema='staging_rscm', con = conn_dwh_sqlserver, if_exists='append',index=False)
            print('success update dan insert')
        except Exception as e:
            print(e)

t1 = time.time()
total=t1-t0
print(total)
text=f'scheduler tanggal : {date}'
print(text)

conn_dwh_sqlserver.close()
conn_his.close()
sys.stdout.close()