from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine
import pandas as pd
import pyodbc
import datetime as dt
date = dt.datetime.today()
import time
import sys

sys.stdout = open("C:/TestPython/DWH_SQL_Server/DWH/logs/LogCekFactStayIndEHR.txt","w")
t0 = time.time()

ehr = create_engine('mysql://hanif-ppi:hanif2022@172.16.19.11/ehr')
dwh_sql_server = create_engine('mssql+pyodbc://andhi-ppi:Andhi2022!@172.16.19.36/DWH_RSCM?driver=SQL+Server+Native+Client+11.0', fast_executemany=True)
try :
    conn_ehr = ehr.connect()
    conn_dwh_sqlserver = dwh_sql_server.connect()
    print('successfully connect to EHR & DWH SQL Server database')
except SQLAlchemyError as e:
    error = str(e.__dict__['orig'])
    print(error)

# tarik query, masuk ke variabel source
source = pd.read_sql_query(""" select 
                                a.PatientID as PatientID,
                                a.AdmissionID as AdmissionID,
                                a.AdmissionDate as AdmissionDate,
                                b.PatientID as PatientIDNull
                            from dwhrscm_talend.FactPatientOrder a 
                            left join dwhrscm_talend.FactPatientStay b on a.PatientID = b.PatientID and a.AdmissionID = b.AdmissionID
                            where b.PatientID is null
                            order by a.PatientID, a.AdmissionID """, conn_dwh_sqlserver)

# ambil key dari source
patientidold = tuple(source["PatientID"].unique()) 
admissionidold = tuple(source["AdmissionID"].unique())
print(patientidold)

# cek StayInd berdasarkan patientid dan admissionid
query_cek = f"SELECT DISTINCT adm.patient_id as PatientID, adm.admission_id as AdmissionID, p.patient_ext_id as MedicalNo, adm.admission_dttm as AdmissionDate, CASE WHEN c.patient_id IS NULL then 'n' ELSE c.StayInd END AS StayInd FROM xocp_ehr_patient_admission adm LEFT JOIN xocp_ehr_patient p ON p.patient_id = adm.patient_id LEFT JOIN (SELECT b.patient_id, b.admission_id, b.created_dttm, CASE WHEN b.StayInd LIKE '%%y%%' THEN 'y' ELSE 'n' END AS StayInd FROM (SELECT ps.patient_id, ps.admission_id, GROUP_CONCAT(ps.StayInd ORDER BY ps.StayInd) as StayInd, MAX(ps.created_dttm) as created_dttm FROM (SELECT patient_id, admission_id, created_dttm, CASE WHEN in_org_id != 104 THEN 'y' WHEN in_org_id = 104 AND TIMEDIFF(stop_dttm,start_dttm) >= '08:00:00' THEN 'y' ELSE 'n' END AS StayInd FROM xocp_ehr_patient_stay WHERE status_cd NOT IN ('nullified') ORDER BY patient_id, admission_id) ps GROUP BY ps.patient_id,ps.admission_id) b ) c ON adm.patient_id = c.patient_id and adm.admission_id = c.admission_id WHERE adm.patient_id IN {patientidold} AND adm.admission_id in {admissionidold} AND adm.org_id in (select org_id from xocp_orgs where parent_id not in ('687','1872') and org_id not in ('687','1872')) ORDER BY adm.patient_id,adm.admission_id"
source = pd.read_sql_query(query_cek, conn_ehr)

source['Flag'] = 1
print(source)

# query buat narik data dari target lalu filter berdasarkan primary key
if source.empty:
    print('tidak ada row yang bisa di proses karena source kosong')
else:
    if len(source) == 1:
        patientid = source["PatientID"].values[0]
        admissionid = source["AdmissionID"].values[0]
        query = f'SELECT PatientID,AdmissionID,MedicalNo,AdmissionDate,StayInd from dwhrscm_talend.FactPatientStay where PatientID IN ({patientid}) AND AdmissionID IN ({admissionid}) AND Flag = 1 order by PatientID, AdmissionID'
        target = pd.read_sql_query(query, conn_dwh_sqlserver)
    else:
        patientid = tuple(source["PatientID"]) 
        admissionid = tuple(source["AdmissionID"])
        query = f'SELECT PatientID,AdmissionID,MedicalNo,AdmissionDate,StayInd from dwhrscm_talend.FactPatientStay where PatientID IN {patientid} AND AdmissionID IN {admissionid} AND Flag = 1 order by PatientID, AdmissionID'
        target = pd.read_sql_query(query, conn_dwh_sqlserver)

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
        inserted.to_sql('FactPatientStay', schema='dwhrscm_talend', con = conn_dwh_sqlserver, if_exists='append',index=False)
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
            df.to_sql(temp_table, schema = 'dwhrscm_talend', con = conn_dwh_sqlserver, if_exists='replace',index = False)
            update_stmt_1 = f'UPDATE t '
            update_stmt_2 = f'SET '
            update_stmt_3 = ", ".join(list_col)
            update_stmt_4 = f' FROM dwhrscm_talend.{table} t '
            update_stmt_5 = f'INNER JOIN (SELECT * from dwhrscm_talend.{temp_table}) AS s ON t.{pk_1} = s.{pk_1} AND t.{pk_2} = s.{pk_2} AND t.{pk_3} = s.{pk_3} '
            update_stmt_6 = f'WHERE t.{pk_1} = s.{pk_1} AND t.{pk_2} = s.{pk_2} AND t.{pk_3} = s.{pk_3}'
            update_stmt_7 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_4 + update_stmt_5 +  update_stmt_6 +";"
            delete_stmt_1 = f'DROP TABLE dwhrscm_talend.{temp_table}'
            print(update_stmt_7)
            print('\n')
            conn_dwh_sqlserver.execute(update_stmt_7)
            conn_dwh_sqlserver.execute(delete_stmt_1)

        try:
            # call fungsi update
            updated_to_sql(modified, 'FactPatientStay', 'PatientID','AdmissionID','Flag')
            # masukkan data yang baru ke table target
            inserted.to_sql('FactPatientStay', schema='dwhrscm_talend', con = conn_dwh_sqlserver, if_exists='append',index=False)
            print('success update dan insert')
        except Exception as e:
            print(e)

t1 = time.time()
total=t1-t0
print(total)
text=f'scheduler tanggal : {date}'
print(text)

conn_dwh_sqlserver.close()
conn_ehr.close()
sys.stdout.close()