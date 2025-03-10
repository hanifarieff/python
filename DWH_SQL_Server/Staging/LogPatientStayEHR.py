import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pandas as pd
import pyodbc
import numpy as np
import datetime as dt
date = dt.datetime.today()
import time

# bikin log ke file
sys.stdout = open("C:/TestPython/DWH_SQL_Server/Staging/logs/LogRowPatientStay.txt","w")
text=f'scheduler tanggal : {date}'
print(text)
t0 = time.time()

# bikin koneksi ke db
conn_ehr_live = db_connection.create_connection(db_connection.ehr_live)
conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)
conn_dwh_sqlserver = db_connection.create_connection(db_connection.dwh_sqlserver)

# source dari ehr live
query = f""" select 
                ps.patient_id as PatientID, 
                ps.admission_id as AdmissionID,
                ps.location_id as LocationID,
                pat.patient_ext_id as MedicalNo,
                loc.sensus as Sensus,
                DATE(ps.created_dttm) AS CreatedDate,
                1 as FlagDB
                -- 25 Jan 2024
                from  -- xocp_ehr_patient_admission pa 
                xocp_ehr_patient_stay ps -- on ps.patient_id= pa.patient_id and ps.admission_id= pa.admission_id
                left join xocp_ehr_bed b USING(bed_id) LEFT JOIN xocp_ehr_obj c ON c.obj_id = ps.location_id
                left join xocp_ehr_obj obj on obj.obj_id = ps.proc_id
                left join xocp_ehr_location loc on loc.location_id = b.location_id and ps.in_org_id = loc.org_id
                left join xocp_ehr_patient pat on pat.patient_id = ps.patient_id
                where
                -- ps.order_id IN ('00220000314483','00220000314863')
                -- ps.created_dttm >= '2025-02-16 00:00:00' and ps.created_dttm <= '2025-02-18-23:59:59' 
                ps.created_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 10 DAY), "%%Y-%%m-%%d 00:00:00") 
                and ps.created_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 23:59:59")
                and
                ps.in_org_id in (select org_id from xocp_orgs where parent_id not in ('687','1872') and org_id not in ('687','1872'))
                and ps.location_id <> '0' and ps.proc_id <> '0' 
                and b.bed_id is not null and b.bed_id <> '0' and c.obj_nm is not null
                -- and ps.order_id IN ('00220000314483','00220000314863')
            """
source = pd.read_sql_query(query, conn_ehr_live)
print('ini source dari live')
print(source)

# location = pd.read_sql_query(""" 
#                                 SELECT LocationID, Sensus
#                                 FROM dwhrscm_talend.DimLocation
#                                 WHERE ScdActive = '1'
#                             """, conn_dwh_sqlserver)
# source = source.merge(location, on='LocationID', how='left')

# new_order_columns = ['ExtractDate','CreatedDate','MedicalNo','FlagDB','Sensus']
# source = source.reindex(columns=new_order_columns)

# ubah yg null jadi string 'null'
source['Sensus'] = source['Sensus'].apply(lambda x: 'null' if pd.isna(x) else str(int(x)))

print(source)

dummy = pd.read_sql_query(""" SELECT TRIM(MedicalNo) AS MedicalNo FROM staging_rscm.DimensionDummyPatient """, conn_staging_sqlserver)

source = source[~source['MedicalNo'].isin(dummy['MedicalNo'])]

print('setelah di filter patient dummy')
print(source)

source = source.groupby(['CreatedDate','FlagDB','Sensus'],dropna=False).size().reset_index(name='RowTotal')
print('setelah di groupby dan count by sensus')
print(source)


source['CreatedDate'] = pd.to_datetime(source.CreatedDate, format='%Y-%m-%d')
source['CreatedDate'] = source['CreatedDate'].dt.strftime('%Y-%m-%d')

if source.empty:
    print('tidak ada data dari source')
else:
    created_date = tuple(source["CreatedDate"].unique())
    flag_db = tuple(source["FlagDB"].unique())
    sensus = tuple(source["Sensus"].unique())

    # bikin function remove comma jika ada variable yang isinya cuma 1, variable ini akan di pakai IN CLAUSE di query target
    def remove_comma(x):
        if len(x) == 1:
            return str(x).replace(',','')
        else:
            return x
    
    created_date = remove_comma(created_date)
    flag_db = remove_comma(flag_db)
    sensus = remove_comma(sensus)


    query = f'SELECT CreatedDate, FlagDB, Sensus, RowTotal from dwhrscm_talend.LogPatientStay where CreatedDate IN {created_date} AND FlagDB IN {flag_db} AND Sensus IN {sensus} order by CreatedDate,FlagDB,Sensus'
    target = pd.read_sql_query(query, conn_dwh_sqlserver)
    # print(target['ExtractDate'])

    target['CreatedDate'] = pd.to_datetime(target.CreatedDate, format='%Y-%m-%d',errors='coerce')
    target['CreatedDate'] = target['CreatedDate'].dt.strftime('%Y-%m-%d')
    print('ini target')
    print(target)

    # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

    # ambil data yang update dari changes
    modified = changes[changes[['CreatedDate','FlagDB','Sensus']].apply(tuple,1).isin(target[['CreatedDate','FlagDB','Sensus']].apply(tuple,1))]
    total_row_upd = len(modified)
    text_upd = f'total row update : {total_row_upd}'
    print(text_upd)
    print(modified)

    # ambil data yang new dari changes
    inserted = changes[~changes[['CreatedDate','FlagDB','Sensus']].apply(tuple,1).isin(target[['CreatedDate','FlagDB','Sensus']].apply(tuple,1))]
    total_row_ins = len(inserted)
    text_ins = f'total row inserted : {total_row_ins}'
    print(text_ins)
    print(inserted)

    if modified.empty:
        if inserted.empty:
            print('there is no data updated and inserted')
        else:
            inserted.to_sql('LogPatientStay', schema='dwhrscm_talend', con = conn_dwh_sqlserver, if_exists = 'append', index=False)
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
                if col == pk_1 or col == pk_2 or col == pk_3 :
                    continue
                list_col.append(f'r.{col} = t.{col}')
            df.to_sql(temp_table,schema = 'dwhrscm_talend',con=conn_dwh_sqlserver, if_exists='replace',index=False)
            update_stmt_1 = f'UPDATE r '
            update_stmt_2 = f'SET '
            update_stmt_3 = ", ".join(list_col)
            update_stmt_8 = f' , r.UpdatedDateDWH = CONVERT(DATETIME2(0), GETDATE(),120)'
            update_stmt_4 = f' FROM dwhrscm_talend.{table} r '
            update_stmt_5 = f'INNER JOIN (SELECT * FROM dwhrscm_talend.{temp_table}) as t ON r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} AND r.{pk_3} = t.{pk_3} '
            update_stmt_6 = f'WHERE r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} AND r.{pk_3} = t.{pk_3} '
            update_stmt_7 = update_stmt_1 + update_stmt_2 + update_stmt_3  +  update_stmt_8 +  update_stmt_4 + update_stmt_5 + update_stmt_6 +";"
            delete_stmt_1 = f'DROP TABLE dwhrscm_talend.{temp_table}'
            print(update_stmt_7)
            conn_dwh_sqlserver.execute(update_stmt_7)
            conn_dwh_sqlserver.execute(delete_stmt_1)

        try:
            # update data
            updated_to_sql(modified, 'LogPatientStay', 'CreatedDate', 'FlagDB','Sensus')

            # insert data baru
            today = dt.datetime.now()
            today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
            inserted['InsertedDateDWH'] = today_convert
            inserted.to_sql('LogPatientStay', schema='dwhrscm_talend', con=conn_dwh_sqlserver, if_exists ='append',index=False)
            print('success update and insert all data')
        
        except Exception as e:
            print(e)

t1 = time.time()
total=t1-t0
print(total)

text= f'scheduler tanggal : {date}'
print(text)
sys.stdout.close()

db_connection.close_connection(conn_ehr_live)
db_connection.close_connection(conn_staging_sqlserver)
db_connection.close_connection(conn_dwh_sqlserver)
sys.stdout.close()