""" File ini berfungsi untuk menarik data `Hasil Form Radioterapi` yang ada di RME """
import psutil
import os
import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pandas as pd
import numpy as np
import pyodbc
import datetime as dt
import time
import json
import re

sys.stdout = open("C:/TestPython/DWH_SQL_Server/Staging/logs/LogTransResultFormRadiotherapy.txt","w")

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
                            a.order_id as OrderID,
                            a.patient_id as PatientID,
                            a.admission_id as AdmissionID,
                            b.obj_id as ObjID,
                            d.obj_nm as ObjName,
                            a.obs_dttm ObservationDate,
                            CASE
                                WHEN b.obs_value_long_ind = '1' THEN c.obs_value 
                                ELSE 
                                    CASE
                                        WHEN b.obj_id IN (69766,69767,69773) THEN REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(e.param_script, ';',b.obs_value+1),'"',-2),'"','')
                                        ELSE b.obs_value 
                                    END
                            END Result,
                            a.status_cd as StatusForm,
                            a.created_user_id as CreatedUserID
                        FROM xocp_his_patient_obs_order a 
                        LEFT JOIN xocp_his_patient_obs_value b on a.order_id = b.order_id and a.patient_id = b.patient_id and a.admission_id = b.admission_id and a.panel_id=b.panel_id 
                        LEFT join xocp_his_patient_obs_value_long c on b.order_id = c.order_id and b.obj_id=c.obj_id
                        LEFT JOIN xocp_obj d on b.obj_id = d.obj_id
                        LEFT JOIN xocp_his_variables e on b.obj_id = e.obj_id 
                        WHERE b.panel_id = '2172' 
                        -- AND a.order_id IN (157648610,157880057,158117903)
                        AND (a.obs_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 20 DAY), "%%Y-%%m-%%d 00:00:00") 
                        AND a.obs_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 23:59:59"))
                        -- AND a.obs_dttm >= '2024-11-01 00:00:00' and a.obs_dttm <= '2024-11-04 23:59:59'
                        -- a.patient_id = 1809059 and a.admission_id = 28
                        -- and b.obj_id = 69763
                    """
    return pd.read_sql_query(query_source,conn_his_live)

def create_column_satusehat(source):
    """ bikin kolom baru SatuSehatCancerResult""" 
    source['SatuSehatCancerResult'] = source.apply(
    lambda row: max(
        [num for num in map(int, row['Result'].split('|')) if num != 9] if row['ObjID'] == 69763 else map(int, row['Result'].split('|')),
        default=0
    ) if row['ObjID'] in [69763, 69764] else 0,
    axis=1
)

    new_order_columns = ['OrderID', 'PatientID', 'AdmissionID', 'ObservationDate', 'ObjID','SequenceID',
                        'ObjName', 'Checkup','CheckupDate','Result','SatuSehatCancerResult',
                        'StatusForm','CreatedUserID']
    source= source.reindex(columns = new_order_columns)

    return source

def validate_json(json_string):
    try:
        return json.loads(json_string)
    except json.decoder.JSONDecodeError as e:
        print(f"Invalid JSON: {e}")
        return None
    
def transform_row(row):
    """ Transform value JSON pada kolom `Result` 
        dipecah menjadi beberapa row, lalu tambah kolom `SequenceID` sebagai primary key
        dari isi kolom Result yang berisi JSON
    """
    if row['ObjID'] == 54774:
        json_data = validate_json(row['Result'])
        if json_data is None:
            # If JSON is invalid, return None
            return None
        
        transformed_rows = []
        for idx, item in enumerate(json_data['body'], 1):
            transformed_rows.append({
                'OrderID': row['OrderID'],
                'PatientID': row['PatientID'],
                'AdmissionID': row['AdmissionID'],
                'ObservationDate' : row['ObservationDate'],
                'ObjID': row['ObjID'],
                'SequenceID': idx,
                'ObjName' : row['ObjName'],
                'Checkup': item[0],
                'CheckupDate': item[1].strip(),
                'Result': item[2],
                'SatuSehatCancerResult': row['SatuSehatCancerResult'],
                'StatusForm': row['StatusForm'],
                'CreatedUserID' : row['CreatedUserID']
            })
        return pd.DataFrame(transformed_rows)
    else:
        return pd.DataFrame([{
            'OrderID': row['OrderID'],
            'PatientID': row['PatientID'],
            'AdmissionID': row['AdmissionID'],
            'ObservationDate' : row['ObservationDate'],
            'ObjID': row['ObjID'],
            'SequenceID': 1,
            'ObjName' : row['ObjName'],
            'Checkup': '-',
            'CheckupDate': '-',
            'Result': row['Result'],
            'SatuSehatCancerResult': row['SatuSehatCancerResult'],
            'StatusForm': row['StatusForm'],
            'CreatedUserID' : row['CreatedUserID']
        }])

def transform_regex(obj_id,conn_his_live, source):
    """ Transform kolom param_script untuk mengambil definisi dari setiap ID,
        Misal `$VAL_OPTION["0"] = "0. baju";` yang diambil `0 = baju`
    """
    query_variables = """ SELECT obj_id, param_script FROM xocp_his_variables
                    where obj_id = %s """
    variables = pd.read_sql_query(query_variables, conn_his_live,params=[obj_id])

    # Extract the mapping from variables and clean it
    mapping_str = variables['param_script'].values[0]

    # Use regular expressions to extract key-value pairs
    pattern = r'\$VAL_OPTION\["(\d+)"\] = "(.*?)";'
    matches = re.findall(pattern, mapping_str)

    # Create a dictionary from the extracted key-value pairs
    mapping_dict = dict(matches)

    # Function to map obsvalue to descriptions
    def map_obsvalue(obsvalue, mapping_dict):
        """ mapping obs value berdasarkan `mapping dict` yang sudah dibuat"""
        # ambil karakter dari obsvalue, contohnya 1|2|3 jadi (1,2,3) lalu baru dapat valuenya dari mapping_dict
        values = obsvalue.split('|')
        descriptions = [mapping_dict.get(value, value) for value in values]
        descriptions = ', '.join(descriptions)
        return descriptions
    
    source['Result'] =  source.apply(
        lambda row: map_obsvalue(row['Result'], mapping_dict) if row['ObjID'] == obj_id else row['Result'], axis=1
    )
    
    source['Result'] = source['Result'].str.replace('\t', ' ').str.replace('\\n','\n')

    return source

def fetch_target_data(source, conn_staging_sqlserver):
    """ ambil data dari tabel target, yaitu TransResultFormRadiotherapy """
    target = pd.DataFrame(columns=['OrderID', 'PatientID', 'AdmissionID', 'ObservationDate', 'ObjID','SequenceID',
                                    'ObjName', 'Checkup','CheckupDate','Result','SatuSehatCancerResult',
                                    'StatusForm','CreatedUserID'])
    query_target = """SELECT 
                        OrderID, 
                        PatientID, 
                        AdmissionID, 
                        ObservationDate, 
                        ObjID,
                        SequenceID,
                        ObjName, 
                        Checkup,
                        CheckupDate,
                        Result,
                        SatuSehatCancerResult,
                        StatusForm,
                        CreatedUserID 
                      FROM staging_rscm.TransResultFormRadioTherapy 
                      WHERE OrderID = ? AND PatientID = ? AND AdmissionID = ?
                      AND ObjID = ? AND SequenceID = ? 
                      ORDER BY PatientID"""
    
    with conn_staging_sqlserver.connect() as conn:
        for index, row in source.iterrows():
            pk_values = (row['OrderID'], row['PatientID'],row['AdmissionID'],row['ObjID'],row['SequenceID'])
            
            # jalankan query dengan parameter pk_values
            results = conn.execute(query_target, pk_values).fetchall()
            
            # jika ada hasilnya maka masukkan ke dataframe target
            if results:
                result_df = pd.DataFrame.from_records(results, columns=target.columns)
                target = pd.concat([target, result_df], ignore_index=True)
    
    return target
    
def detect_changes(source, target):
    """ deteksi perubahan antara dataframe `source` dan `target` """
    
    # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

    # ambil data yang update
    modified = changes[changes[['OrderID','PatientID','AdmissionID','ObjID','SequenceID']].apply(tuple,1).isin(target[['OrderID','PatientID','AdmissionID','ObjID','SequenceID']].apply(tuple,1))]

    # ambil data yang baru
    inserted = changes[~changes[['OrderID','PatientID','AdmissionID','ObjID','SequenceID']].apply(tuple,1).isin(target[['OrderID','PatientID','AdmissionID','ObjID','SequenceID']].apply(tuple,1))]

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
            f' WHERE t.{key_1} = s.{key_1} AND t.{key_2} = s.{key_2} AND t.{key_3} = s.{key_3} AND t.{key_4} = s.{key_4} AND t.{key_5} = s.{key_5};'
        )
        delete_stmt = f'DROP TABLE staging_rscm.{temp_table};'
        print(update_stmt)
        
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
            today = dt.datetime.now()
            today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
            inserted['InsertDateStaging'] = today_convert
            inserted.to_sql('TransResultFormRadiotherapy', schema='staging_rscm', con=conn_staging_sqlserver, if_exists='append', index=False)
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

        # Buat kolom baru SatuSehatCancerResult
        source = create_column_satusehat(source)
        print('setelah ditambahkan kolom baru')
        print(source.loc[:,['Result','SatuSehatCancerResult']])

        # Gabungkan source dengan fungsi transform_row lalu masukkan ke dalam variable result_df
        source = pd.concat(source.apply(transform_row, axis=1).to_list(), ignore_index=True)

        # Menjalankan fungsi transform_regex, masukkan parameter obj_id dibawah ini lalu di looping
        list_obj_id = [69763,69764] 
        for obj_id in list_obj_id:
            transform_regex(obj_id,conn_his_live, source)

        print('setelah dijalankan fungsi transform regex')
        print(source)

        # ambil data dari database target
        target = fetch_target_data(source, conn_staging_sqlserver)
        print(target)

        # Deteksi perubahan (buat dapetin modified dan inserted)
        modified, inserted = detect_changes(source, target)
        print("Changes Detected:")
        print("Modified Data:")
        print(modified)
        print("Inserted Data:")
        print(inserted)

        # update data
        updated_data(modified, 'TransResultFormRadiotherapy', 'OrderID','PatientID','AdmissionID','ObjID','SequenceID', conn_staging_sqlserver)

        # insert data
        inserted_data(inserted, conn_staging_sqlserver)

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

