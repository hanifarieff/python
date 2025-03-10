import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pandas as pd
import pyodbc
import datetime as dt
date = dt.datetime.today()
import time
import sys

sys.stdout = open("C:/TestPython/DWH_SQL_Server/Staging/logs/LogTransPatientBMI.txt","w")

t0 = time.time()

conn_his_live = db_connection.create_connection(db_connection.his_live)
conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)

source = pd.read_sql_query(""" 
                            SELECT
                                a.order_id as OrderObsID,
                                y.PatientID,
                                y.AdmissionID,
                                y.ObservationDate,
                                CASE
                                    WHEN c.panel_nm IS NULL OR c.panel_nm = ' ' THEN '-'
                                    ELSE c.panel_nm
                                END AS PanelName,
                                MAX(CASE WHEN a.obj_id = 41923 THEN CASE WHEN a.obs_value_long_ind = '1' THEN b.obs_value ELSE a.obs_value END END) AS BMI,
                                MAX(CASE WHEN a.obj_id = 41923 THEN a.status_cd END) AS BMIStatus,
                                MAX(CASE WHEN a.obj_id = 42022 THEN CASE WHEN a.obs_value_long_ind = '1' THEN b.obs_value ELSE a.obs_value END END) AS BMIDetail,
                                MAX(CASE WHEN a.obj_id = 42022 THEN a.status_cd END) AS BMIDetailStatus
                            FROM
                            (
                                SELECT 
                                    x.PatientID,
                                    x.AdmissionID,
                                    MAX(x.ObservationDate) AS ObservationDate
                                    FROM 
                                    (
                                        SELECT  
                                            a.order_id as OrderObsID,
                                            a.patient_id as PatientID,
                                            a.admission_id as AdmissionID,
                                            a.obs_dttm as ObservationDate,
                                            CASE
                                                WHEN c.panel_nm IS NULL OR c.panel_nm = ' ' THEN '-'
                                                ELSE c.panel_nm
                                            END AS PanelName,
                                            MAX(CASE WHEN a.obj_id = 41923 THEN CASE WHEN a.obs_value_long_ind = '1' THEN b.obs_value ELSE a.obs_value END END) AS BMI,
                                            MAX(CASE WHEN a.obj_id = 41923 THEN a.status_cd END) AS BMIStatus,
                                            MAX(CASE WHEN a.obj_id = 42022 THEN CASE WHEN a.obs_value_long_ind = '1' THEN b.obs_value ELSE a.obs_value END END) AS BMIDetail,
                                            MAX(CASE WHEN a.obj_id = 42022 THEN a.status_cd END) AS BMIDetailStatus
                                        from xocp_his_patient_obs_value a
                                        left join xocp_his_patient_obs_value_long b on a.order_id = b.order_id and a.obj_id = b.obj_id 
                                        left join xocp_his_panel c on a.panel_id = c.panel_id
                                        left join xocp_obj d on a.obj_id = d.obj_id
                                        where a.obj_id in (41923,42022)
                                        and a.obs_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 4 DAY), "%%Y-%%m-%%d 00:00:00") 
                                        and a.obs_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 23:59:59")
                                        -- and a.patient_id IN (1530156, 1101075) and a.admission_id IN(1,40)
                                        -- and a.order_id = 136544591
                                        -- and a.status_cd = 'normal'
                                        GROUP BY a.patient_id,
                                            a.admission_id,
                                            a.obs_dttm,
                                            c.panel_nm
                                    ) x
                                GROUP BY x.PatientID,x.AdmissionID
                            ) y
                        LEFT JOIN xocp_his_patient_obs_value a on a.patient_id = y.PatientID and a.admission_id = y.AdmissionID and a.obs_dttm = y.ObservationDate and a.obj_id IN (41923,42022)
                        LEFT join xocp_his_patient_obs_value_long b on a.order_id = b.order_id and a.obj_id = b.obj_id
                        LEFT join xocp_his_panel c on a.panel_id = c.panel_id
                        GROUP BY y.PatientID,y.AdmissionID
                        ORDER BY y.PatientID, y.AdmissionID 
                    """, conn_his_live)
print(source)

if source.empty:
    print('tidak ada data dari source')
else:
    # ambil primary key dari source, pake unique biar tidak duplicate
    OrderObsID = tuple(source["OrderObsID"].unique())

     # bikin function remove comma jika ada variable yang isinya cuma 1, variable ini akan di pakai IN CLAUSE di query target
    def remove_comma(x):
        if len(x) == 1:
            return str(x).replace(',','')
        else:
            return x
    
    OrderObsID = remove_comma(OrderObsID)
     
    # query buat narik data dari target lalu filter berdasarkan primary key
    query = f'SELECT OrderObsID, PatientID, AdmissionID, ObservationDate, PanelName, BMI,BMIStatus, BMIDetail,BMIDetailStatus FROM staging_rscm.TransPatientBMI WHERE OrderObsID IN {OrderObsID}'
    target = pd.read_sql_query(query, conn_staging_sqlserver)

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
            inserted.to_sql('TransPatientBMI', schema='staging_rscm' ,con=conn_staging_sqlserver, if_exists = 'append', index=False)
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
            df.to_sql(temp_table, schema='staging_rscm', con=conn_staging_sqlserver, if_exists = 'replace', index = False)
            update_stmt_1 = f'UPDATE t '
            update_stmt_2 = f'SET '
            update_stmt_3 = ", ".join(a)
            update_stmt_8 = f' , t.UpdateDateStaging = CONVERT(DATETIME2(0), GETDATE(),120)'
            update_stmt_4 = f' FROM staging_rscm.{table} t '
            update_stmt_5 = f'INNER JOIN (SELECT * FROM staging_rscm.{temp_table}) AS s ON t.{pk_1} = s.{pk_1}  '
            update_stmt_6 = f' WHERE t.{pk_1} = s.{pk_1} '
            update_stmt_7 = update_stmt_1 +  update_stmt_2 + update_stmt_3 + update_stmt_8 + update_stmt_4 + update_stmt_5 + update_stmt_6 +";"
            delete_stmt_1 = f'DROP TABLE staging_rscm.{temp_table} '
            print(update_stmt_7)
            conn_staging_sqlserver.execute(update_stmt_7)
            conn_staging_sqlserver.execute(delete_stmt_1)

        try:
            #update data
            updated_data(modified, 'TransPatientBMI', 'OrderObsID')

            #insert data
            inserted.to_sql('TransPatientBMI', schema='staging_rscm', con=conn_staging_sqlserver, if_exists = 'append', index=False)

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

db_connection.close_connection(conn_his_live)
db_connection.close_connection(conn_staging_sqlserver)
sys.stdout.close()


