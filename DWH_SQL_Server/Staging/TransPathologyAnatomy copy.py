import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pandas as pd
import pyodbc
import numpy as np
import datetime as dt
date = dt.datetime.today()
import time
import psutil
import os
import json
import re

sys.stdout = open("C:/TestPython/DWH_SQL_Server/Staging/logs/LogTransPathologyAnatomyNew.txt","w")

# Get the current process info
process = psutil.Process(os.getpid())
""" Info code process"""

# Measure memory usage before code execution
memory_before = process.memory_info().rss / (1024 * 1024)  # In MB
""" Hitung Memori Awal sebelum code running"""
print(f"Memory before: {memory_before} MB")

t0 = time.time()
""" Waktu awal sebelum code running"""

def get_connections():
    """ Membuat koneksi ke database """
    conn_his_live = db_connection.create_connection(db_connection.his_live)
    conn_his = db_connection.create_connection(db_connection.replika_his)
    conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)
    return conn_his_live, conn_his, conn_staging_sqlserver

def get_source_data(conn_his_live):
    """ Extract data dari database sumber (HIS)""" 
    query_source = f""" 
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
                                            WHEN b.obj_id IN (56308,56310,56349,56309) THEN REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(c.param_script, ';',b.obs_value+1),'"',-2),'"','')
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
                            -- a.created_dttm >= '2024-07-21 00:00:00' and a.created_dttm <= '2024-07-31 23:59:59'
                            -- a.no_pa_id = 'H24-02103'
                            ORDER BY a.created_dttm
                    """
    source = pd.read_sql_query(query_source,conn_his_live)
    source['ObsValue'] = source['ObsValue'].str.replace('\t', ' ').str.replace('\\n','\n')
    source['FinalUserID'] = source['FinalUserID'].replace(np.nan,0).astype('int64')
    source['ObjID'].replace({np.nan:0}, inplace=True)

    return source

def transform_regex(conn_his_live):
    """ Transform kolom param_script untuk mengambil definisi dari setiap ID,
        Misal `$VAL_OPTION["0"] = "0. baju";` yang diambil `0 = baju`
        outputnya adalah variabel `mapping_dict`
    """
    query_variables = f""" SELECT obj_id, param_script FROM xocp_his_variables
                    where obj_id = 55606 """
    variables = pd.read_sql_query(query_variables, conn_his_live)

    # Extract the mapping from variables and clean it
    mapping_str = variables['param_script'].values[0]

    # Use regular expressions to extract key-value pairs
    pattern = r'\$VAL_OPTION\["(\d+)"\] = "(.*?)";'
    matches = re.findall(pattern, mapping_str)

    # Create a dictionary from the extracted key-value pairs
    mapping_dict = dict(matches)

    return mapping_dict

def map_obsvalue(obsvalue, mapping_dict):
    """ mapping obs value berdasarkan `mapping dict` yang sudah dibuat"""
    # ambil karakter dari obsvalue pake regex, contoh '0^1|1^10|3^10' jadi [(0,1),(1,10),(3,10)]
    value = re.findall(r'(\d+)\^(\d+)',obsvalue)
    descriptions = [f"{mapping_dict.get(a, a)} = {b}" for a,b in value]
    return ', '.join(descriptions)

def get_target_data(source, conn_staging_sqlserver):
    """ ambil data dari tabel target, yaitu TransPathologyAnatomy """
    target = pd.DataFrame(columns=['RegistrationNo','OrderID','PatientID','AdmissionID','PanelID','PanelName', 'ObjID',
                                   'ObjName','ObsValue','Status','CreatedUserID','CreatedDate','FinalUserID','FinalDate'])
    query_target = """SELECT 
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
                      FROM staging_rscm.TransPathologyAnatomy 
                      WHERE RegistrationNo = ? AND OrderID = ? AND PatientID = ? AND AdmissionID = ?
                      AND ObjID = ?
                      ORDER BY RegistrationNo"""
    
    with conn_staging_sqlserver.connect() as conn:
        for index, row in source.iterrows():
            pk_values = (row['RegistrationNo'],row['OrderID'], row['PatientID'],row['AdmissionID'],row['ObjID'])
            
            # jalankan query dengan parameter pk_values
            results = conn.execute(query_target, pk_values).fetchall()
            
            # jika ada hasilnya maka masukkan ke dataframe target
            if results:
                result_df = pd.DataFrame.from_records(results, columns=target.columns)
                target = pd.concat([target, result_df], ignore_index=True)
            else:
                print('The results are empty')
    
    return target
    
def detect_changes(source, target):
    """ deteksi perubahan antara dataframe `source` dan `target` """
    
    # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

    # ambil data yang update
    modified = changes[changes[['RegistrationNo','OrderID','PatientID','AdmissionID','ObjID']].apply(tuple,1).isin(target[['RegistrationNo','OrderID','PatientID','AdmissionID','ObjID']].apply(tuple,1))]

    # ambil data yang baru
    inserted = changes[~changes[['RegistrationNo','OrderID','PatientID','AdmissionID','ObjID']].apply(tuple,1).isin(target[['RegistrationNo','OrderID','PatientID','AdmissionID','ObjID']].apply(tuple,1))]

    return modified, inserted

def updated_data(df, table_name, key_1,key_2,key_3,key_4,key_5, conn_staging_sqlserver):
    """ update data di tabel target """
    if not df.empty:
        a = [f't.{col} = s.{col}' for col in df.columns if col != key_1 and col != key_2 and col != key_3 and col != key_4 and col != key_5]
        temp_table = f'{table_name}_temporary_table'
        
        # Upload the temp table to SQL Server
        df.to_sql(temp_table, schema='staging_rscm', con=conn_staging_sqlserver, if_exists = 'replace', index = False)
        update_stmt = (
            f'UPDATE t SET ' + ", ".join(a) +
            f', t.UpdateDateStaging = CONVERT(DATETIME2(0), GETDATE(),120)'
            f' FROM staging_rscm.{table_name} t '
            f'INNER JOIN (SELECT * FROM staging_rscm.{temp_table}) AS s ON t.{key_1} = s.{key_1} AND t.{key_2} = s.{key_2} AND t.{key_3} = s.{key_3} AND t.{key_4} = s.{key_4} AND t.{key_5} = s.{key_5}'
            f'WHERE t.{key_1} = s.{key_1} AND t.{key_2} = s.{key_2} AND t.{key_3} = s.{key_3} AND t.{key_4} = s.{key_4} AND t.{key_5} = s.{key_5};'
        )
        delete_stmt = f'DROP TABLE staging_rscm.{temp_table};'
        
        with conn_staging_sqlserver.begin() as transaction:
            # Execute update and delete temp table
            conn_staging_sqlserver.execute(update_stmt)
            conn_staging_sqlserver.execute(delete_stmt)
            print('\nData Success Updated')
    else:
        print('\nTidak ada data yang berubah')    

def inserted_data(inserted, conn_staging_sqlserver):
    """ insert data di tabel target """
    if not inserted.empty:
        with conn_staging_sqlserver.begin() as transaction:
            inserted.to_sql('TransPathologyAnatomy', schema='staging_rscm', con=conn_staging_sqlserver, if_exists='append', index=False)
            print('Data Success Inserted')
    else:
        print('Tidak ada data yang baru')

def main():

    """ Fungsi utama untuk menjalankan semua proses"""

    conn_his_live,conn_his, conn_staging_sqlserver = get_connections()

    try:
        # Ambil data dari source
        source = get_source_data(conn_his_live)
        print("Source Data:")
        print(source)

        # Panggil fungsi transform_regex untuk mapping obsvalue lalu taro di mapping_dict
        mapping_dict = transform_regex(conn_his_live)

        # Apply function map_obsvalue
        source['ObsValue'] = source.apply(
        lambda row: map_obsvalue(row['ObsValue'], mapping_dict) if row['ObjID'] == 55606 else row['ObsValue'], axis=1
        )

        # ambil data dari database target
        target = get_target_data(source, conn_staging_sqlserver)
        print("target Data:")
        print(target)

        # # Deteksi perubahan (buat dapetin modified dan inserted)
        # modified, inserted = detect_changes(source, target)
        # print("Changes Detected:")
        # print("Modified Data:")
        # print(modified)
        # print("Inserted Data:")
        # print(inserted)

        # # update data
        # updated_data(modified, 'TransResultFormRadiotherapy', 'OrderID','PatientID','AdmissionID','ObjID','SequenceID', conn_staging_sqlserver)

        # # insert data
        # inserted_data(inserted, conn_staging_sqlserver)

    finally:
        db_connection.close_connection(conn_his)
        db_connection.close_connection(conn_staging_sqlserver)
        db_connection.close_connection(conn_his_live)

# Run the main process
if __name__ == "__main__":
    main()
        
#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)
print('\n')

# Measure memory usage after code execution
memory_after = process.memory_info().rss / (1024 * 1024)  # In MB
""" Memory setelah di code selesai running"""
print(f"Memory after: {memory_after} MB")

# Calculate memory used
memory_used = memory_after - memory_before
""" Total Memori yang terpakai untuk menjalankan program ini"""
print(f"Memory used: {memory_used} MB")

text = dt.datetime.today()
""" tanggal hari ini """
print(f"scheduler tanggal : {text}")

sys.stdout.close()


