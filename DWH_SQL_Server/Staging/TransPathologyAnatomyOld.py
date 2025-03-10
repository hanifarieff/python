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
import re

sys.stdout = open("C:/TestPython/DWH_SQL_Server/Staging/logs/LogTransPathologyAnatomy.txt","w")

t0 = time.time()

conn_his_live = db_connection.create_connection(db_connection.his_live)
conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)

source = pd.read_sql_query(""" 
                            SELECT 
                                a.no_pa_id as RegistrationNo,
                                a.order_id as OrderID,
                                a.patient_id as PatientID,
                                a.admission_id as AdmissionID,
                                a.panel_id as PanelID,
                                e.panel_nm as PanelName,
                                b.obj_id as ObjID,
                                d.obj_nm ObjName,
                                CASE
                                    WHEN b.obs_value_long_ind = '1' then 
                                    CASE
                                        WHEN b.obj_id IN (68724) THEN 
                                            CASE
                                                WHEN SUBSTRING_INDEX(SUBSTRING_INDEX(f.obs_value, '"diag_code":"', -1), '"', 1) LIKE 'NONE%%' THEN
                                                SUBSTRING_INDEX(SUBSTRING_INDEX(f.obs_value, '"diag_naratif":"', -1), '"', 1)
                                                ELSE
                                                REPLACE(CONCAT(
                                                SUBSTRING_INDEX(SUBSTRING_INDEX(f.obs_value, '"diag_name":"', -1), '"', 1), 
                                                ' ', 
                                                SUBSTRING_INDEX(SUBSTRING_INDEX(f.obs_value, '"diag_code":"', -1), '"', 1)
                                                ),'NONE_0','')
                                            END
                                        WHEN b.obj_id IN (68725) THEN REPLACE(CONCAT(
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(f.obs_value, '"diag_naratif":"', -1), '"', 1), 
                                    ' ', 
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(f.obs_value, '"diag_code":"', -1), '"', 1)
                                            ),'NONE_0','')
                                        ELSE f.obs_value
                                        END
                                    ELSE 
                                        CASE
                                           -- ini untuk obj_id yang perlu di translate dari tabel xocp_his_variables kolom param_script
                                            WHEN b.obj_id IN (56308,56310,56349,56309,55607,55608,55609,55610,55613,55600) THEN REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(c.param_script, ';',b.obs_value+1),'"',-2),'"','')
                                            -- ini untuk obj name "Cara Pengambilan" dari form sitopatologi, beda sendiri querynya karena penulisannya outputnya beda dengan obj yang di atas
                                            WHEN b.obj_id IN (55614) THEN CONCAT(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(c.param_script, ';',b.obs_value+1),'"',-2),'"',''),': ' ,SUBSTRING_INDEX(b.obs_value,'^',-1))                 
                                            ELSE b.obs_value
                                        END
                                END AS ObsValue,
                                a.status_cd as Status,
                                a.created_user_id as CreatedUserID,
                                a.created_dttm as CreatedDate,
                                a.final_user_id as FinalUserID,
                                a.final_dttm as FinalDate
                            FROM xocp_his_pathology_anatomy_report a
                            LEFT JOIN xocp_his_patient_obs_value b on a.order_id = b.order_id and a.patient_id = b.patient_id and a.admission_id = b.admission_id AND a.panel_id = b.panel_id
                            LEFT JOIN xocp_his_variables c on b.obj_id = c.obj_id 
                            LEFT JOIN xocp_obj d on b.obj_id = d.obj_id 
                            LEFT JOIN xocp_his_panel e on a.panel_id = e.panel_id
                            LEFT JOIN xocp_his_patient_obs_value_long f on b.order_id = f.order_id and b.obj_id= f.obj_id
                            where 
                            a.created_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 20 DAY), "%%Y-%%m-%%d 00:00:00") 
                            and a.created_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 23:59:59")
                            -- a.created_dttm >= '2025-02-01 00:00:00' and a.created_dttm <= '2025-02-03 23:59:59'
                            -- a.no_pa_id = 'H24-02103'
                            ORDER BY a.created_dttm
                    """, conn_his_live)

print(source)

source['ObsValue'] = source['ObsValue'].str.replace('\t', ' ').str.replace('\\n','\n')
source['FinalUserID'] = source['FinalUserID'].replace(np.nan,0).astype('int64')
source['ObjID'].replace({np.nan:0}, inplace=True)

# Buat ambil value untuk obj_id = 55606
query_variables = f""" SELECT obj_id, param_script FROM xocp_his_variables
                 where obj_id = 55606 """
variables = pd.read_sql_query(query_variables, conn_his_live)

# Extract the mapping from Table B and clean it
mapping_str = variables['param_script'].values[0]

# Use regular expressions to extract key-value pairs
pattern = r'\$VAL_OPTION\["(\d+)"\] = "(.*?)";'
matches = re.findall(pattern, mapping_str)

# Create a dictionary from the extracted key-value pairs
mapping_dict = dict(matches)

# Function to map obsvalue to descriptions
def map_obsvalue(obsvalue, mapping_dict):
    # ambil karakter dari obsvalue pake regex, contoh '0^1|1^10|3^10' jadi [(0,1),(1,10),(3,10)]
    value = re.findall(r'(\d+)\^(\d+)',obsvalue)
    descriptions = [f"{mapping_dict.get(a, a)} = {b}" for a,b in value]
    return ', '.join(descriptions)

# Apply the mapping function
source['ObsValue'] = source.apply(
    lambda row: map_obsvalue(row['ObsValue'], mapping_dict) if row['ObjID'] == 55606 else row['ObsValue'], axis=1
)

print(source['ObsValue'])

# source=source.drop(columns='ObsValue')
# bikin function remove comma jika ada variable yang isinya cuma 1, variable ini akan di pakai IN CLAUSE di query target
def remove_comma(x):
    if len(x) == 1:
        return str(x).replace(',','')
    else:
        return x
    
if source.empty:
    print('tidak ada data dari source')
else:
    # ambil primary key dari source, pake unique biar tidak duplicate
    registration_no = tuple(source["RegistrationNo"].unique())
    order_id = tuple(source["OrderID"].unique())
    patient_id = tuple(source["PatientID"].unique())
    admission_id = tuple(source["AdmissionID"].unique())
    obj_id = tuple(source["ObjID"].unique())

    registration_no = remove_comma(registration_no)
    order_id = remove_comma(order_id)
    patient_id = remove_comma(patient_id)
    admission_id = remove_comma(admission_id)
    obj_id = remove_comma(obj_id)
    
    # query buat narik data dari target lalu filter berdasarkan primary key
    query = f"""SELECT 
                    TRIM(RegistrationNo) as RegistrationNo, 
                    OrderID, 
                    PatientID, 
                    AdmissionID, 
                    PanelID,
                    PanelName, 
                    ObjID,
                    ObjName, 
                    ObsValue,
                    Status,
                    CreatedUserID,
                    CreatedDate,
                    FinalUserID,
                    FinalDate 
                FROM staging_rscm.TransPathologyAnatomy WHERE RegistrationNo IN {registration_no} AND 
                OrderID IN {order_id} AND PatientID IN {patient_id} AND AdmissionID IN {admission_id} AND ObjID IN {obj_id}
            """
    target = pd.read_sql_query(query, conn_staging_sqlserver)

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
            today = dt.datetime.now()
            today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
            inserted['InsertDateStaging'] = today_convert
            inserted.to_sql('TransPathologyAnatomy', schema='staging_rscm' ,con=conn_staging_sqlserver, if_exists = 'append', index=False)
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
            df.to_sql(temp_table, schema='staging_rscm', con=conn_staging_sqlserver, if_exists = 'replace', index = False)
            update_stmt_1 = f'UPDATE t '
            update_stmt_2 = f'SET '
            update_stmt_3 = ", ".join(a)
            update_stmt_8 = f' , t.UpdateDateStaging = CONVERT(DATETIME2(0), GETDATE(),120)'
            update_stmt_4 = f' FROM staging_rscm.{table} t '
            update_stmt_5 = f'INNER JOIN (SELECT * FROM staging_rscm.{temp_table}) AS s ON t.{pk_1} = s.{pk_1} AND t.{pk_2} = s.{pk_2} AND t.{pk_3} = s.{pk_3} AND t.{pk_4} = s.{pk_4} AND t.{pk_5} = s.{pk_5} '
            update_stmt_6 = f' WHERE t.{pk_1} = s.{pk_1}  AND t.{pk_2} = s.{pk_2} AND t.{pk_3} = s.{pk_3} AND t.{pk_4} = s.{pk_4} AND t.{pk_5} = s.{pk_5} '
            update_stmt_7 = update_stmt_1 +  update_stmt_2 + update_stmt_3 + update_stmt_8 + update_stmt_4 + update_stmt_5 + update_stmt_6 +";"
            delete_stmt_1 = f'DROP TABLE staging_rscm.{temp_table} '
            print(update_stmt_7)
            conn_staging_sqlserver.execute(update_stmt_7)
            conn_staging_sqlserver.execute(delete_stmt_1)

        try:
            #update data
            updated_data(modified, 'TransPathologyAnatomy', 'RegistrationNo','OrderID','PatientID','AdmissionID','ObjID')

            #insert data
            today = dt.datetime.now()
            today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
            inserted['InsertDateStaging'] = today_convert
            inserted.to_sql('TransPathologyAnatomy', schema='staging_rscm', con=conn_staging_sqlserver, if_exists = 'append', index=False)

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
db_connection.close_connection(conn_his_live)
sys.stdout.close()


