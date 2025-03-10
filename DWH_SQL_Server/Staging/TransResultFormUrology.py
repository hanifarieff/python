""" File ini berfungsi untuk menarik data `Hasil Form Urology` yang ada di RME `(Form Registri Batu Saluran Kemih)` ke *DB Staging* """
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
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# log path file error
log_file_path = "C:/TestPython/DWH_SQL_Server/Staging/Log/TransResultFormUrology.log"

logging.basicConfig(
    filename = log_file_path,
    level = logging.DEBUG, # Pilih debuh untuk capture semua jenis error
    format =  '%(asctime)s - %(levelname)s - %(message)s',
    # filemode="w" # Selalu update log di file yang sama, tidak insert baru
)

def get_connections():
    """ Membuat koneksi ke database """
    conn_his_live = db_connection.create_connection(db_connection.his_live)
    conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)
    logging.info("success connect to DB!")
    return conn_his_live, conn_staging_sqlserver

def get_source_data(conn_his_live):
    """ Extract data dari database sumber (HIS)""" 
    source = pd.read_sql_query("""
                        SELECT 
                                a.order_id as OrderID,
                                a.patient_id as PatientID,
                                a.admission_id as AdmissionID,
                                a.obs_dttm ObservationDate,
                                b.obj_id as ObjID,
                                d.obj_nm as ObjName,
                                CASE
                                    WHEN b.obs_value_long_ind = '1' THEN c.obs_value 
                                    ELSE 
                                        CASE
                                            -- ini untuk obj Derajat Hidronefrosis, obj lainnya di translate di python
                                            WHEN b.obj_id IN (162830) THEN REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(e.param_script, ';',b.obs_value+1),'"',-2),'"','')
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
                        WHERE b.panel_id = '1653'  AND
                        (a.obs_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 30 DAY), "%%Y-%%m-%%d 00:00:00") 
                        and a.obs_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 23:59:59"))
                        -- a.order_id IN ('160736062')
                    """, conn_his_live)
    
    logging.info("Extracted %s rows from source HIS LIVE", source.shape[0])
    return source


def translate_same_obj(obj_id, conn_his_live, source):
    """ Function ini untuk translate value `Obj 53835 (Lokasi Batu)`, `163039 (Riwayat Penyakit)`,
    Dan `163040 (Keparah Batu Saluran Kemih)` """

    query_variables = """ SELECT obj_id, param_script FROM xocp_his_variables
                    where obj_id = %s """
    variables = pd.read_sql_query(query_variables, conn_his_live, params=[obj_id])

    # Extract the mapping from variables and clean it
    mapping_str = variables['param_script'].values[0]
    
    # Use regular expressions to extract key-value pairs
    # Regex ini untuk extract kalimat Misal `$VAL_OPTION["0"] = "Ginjal kanan";` yang diambil `0`, `Ginjal Kanan`
    pattern = r'\$VAL_OPTION\["(\d+)"\] = "(.*?)";'
    matches = re.findall(pattern, mapping_str)

    # Create a dictionary from the extracted key-value pairs
    mapping_dict = dict(matches)

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
    return source

def translate_different_obj(obj_id, conn_his_live, source):
    """ Fungsi ini untuk obj `163038 = Pemeriksaan Penunjang`, karena isi valuenya agak berbeda dengan Obj yang 
    di Function translate_same_obj
        
    """
    query_variables = """ SELECT obj_id, param_script FROM xocp_his_variables
                    where obj_id = %s """
    variables = pd.read_sql_query(query_variables, conn_his_live, params=[obj_id])

    # Extract the mapping from variables and clean it
    mapping_str = variables['param_script'].values[0]

    # Use regular expressions to extract key-value pairs
    pattern = r'\$VAL_OPTION\["(\d+)"\] = "(.*?)";'
    matches = re.findall(pattern, mapping_str)

    # Create a dictionary from the extracted key-value pairs
    mapping_dict = dict(matches)

    def map_obsvalue(obsvalue, mapping_dict):
        """ Map obsvalue based on the created mapping dictionary """
        # Check jika ada suffix `^` contoh kaya gini 0|2|4^RPG Kanan berarti
        # outputnya "USG, CT Scan Non Kontras, Pemeriksaan lainnya : RPG Kanan"
        if '^' in obsvalue:
            main_part, suffix = obsvalue.split('^', 1)
        else:
            main_part, suffix = obsvalue, ""

        # Split the main part into individual values and map using `mapping_dict`
        values = main_part.split('|')
        descriptions = [mapping_dict.get(value, f"Unknown({value})") for value in values]
        
        # Combine descriptions and add suffix if available
        result = ', '.join(descriptions)
        if suffix:
            result += f" : {suffix}"
        
        return result

    source['Result'] =  source.apply(
    lambda row: map_obsvalue(row['Result'], mapping_dict) if row['ObjID'] == obj_id else row['Result'], axis=1
    )
    return source

def transform_row(row):
    """ Function ini untuk split row jadi beberapa row mengikuti jumlah value yang dimiliki obj_id `[53835, 163040]` """
    split_obj_id = [53835, 163040]

    if row['ObjID'] in split_obj_id:
        result_parts = row['Result'].split(", ")
        return pd.DataFrame({
            'OrderID': [row['OrderID']] * len(result_parts),
            'PatientID': [row['PatientID']] * len(result_parts),
            'AdmissionID': [row['AdmissionID']] * len(result_parts),
            'ObservationDate' : [row['ObservationDate']] * len(result_parts),
            'ObjID': [row['ObjID']] * len(result_parts),
            'SequenceID': range(1, len(result_parts) + 1),
            'ObjName': [row['ObjName']] * len(result_parts),
            'Result': result_parts,
            'StatusForm': [row['StatusForm']] * len(result_parts),
            'CreatedUserID': [row['CreatedUserID']] * len(result_parts)
        })
    else:
        # If ObjID is not in split_obj_ids, return the row as a single DataFrame
        row['SequenceID'] = 1
        return pd.DataFrame([row])
    print()
      
def snomed_id(source):
    """ Function untuk membuat kolom `SnomedID` """
    # buat referensi snomed id dalam bentuk dictionary, menyesuaikan requirement satu sehat
    # source: https://satusehat.kemkes.go.id/platform/docs/id/interoperability/uronefro/
    reference_dict = {
    # Ini Obj Lokasi Batu
    53835: {
        "Ginjal kanan" : "9846003",
        "Ginjal kiri" : "18639004",
        "Ureter kanan" : "25308007",
        "Ureter kiri" : "26559004",
        "Buli-buli" : "89837001",
        "Ureter" : "13648007"
    },
    # Ini Obj Derajat Hidronefrosis
    162830: {
        "Grade 1" : "258351006",
        "Grade 2" : "258352004",
        "Grade 3" : "258353009",
        "Grade 4" : "258354003",
        "Tidak ada hidronefrosis" : "OV000352"
    },
    # Ini Obj Keparahan Batu Saluran Kemih
    163040: {
        "Infeksi Saluran Kemih" : "68566005",
        "Tidak Ada Infeksi" : "55189008",
        "Penyakit Ginjal Kronis" : "709044004"
    }
}
    # bikin function untuk apply snomed id berdasarkan ObjID dan result
    def map_result(row):
        obj_id = row['ObjID']
        result = row['Result']
        return reference_dict.get(obj_id, {}).get(result, '-')

    # def map_result(row):
    #     obj_id = row['ObjID']
    #     result_texts = row['Result'].split(",")
    #     mapped_codes = [reference_dict.get(obj_id,{}).get(text,'-') for text in result_texts]
    
    source['SnomedID'] = source.apply(map_result,axis=1)
    new_order_columns = ['OrderID','PatientID','AdmissionID','ObservationDate','ObjID','SequenceID','ObjName'
                         ,'Result','SnomedID','StatusForm','CreatedUserID']
    source = source.reindex(columns=new_order_columns)
    return source

def fetch_target_data(source, conn_staging_sqlserver):
    """ ambil data dari tabel target, yaitu TransResultFormUrology"""
    target = pd.DataFrame(columns=['OrderID', 'PatientID', 'AdmissionID', 'ObservationDate', 'ObjID',
                                   'SequenceID','ObjName','Result','SnomedID','StatusForm','CreatedUserID'])
    query_target = """SELECT 
                        OrderID, 
                        PatientID, 
                        AdmissionID, 
                        ObservationDate, 
                        ObjID,
                        SequenceID,
                        ObjName, 
                        Result,
                        SnomedID,
                        StatusForm,
                        CreatedUserID 
                      FROM staging_rscm.TransResultFormUrology 
                      WHERE OrderID = ? AND PatientID = ? AND AdmissionID = ?
                      AND ObjID = ? AND SequenceID = ?
                      ORDER BY OrderID"""
    
    with conn_staging_sqlserver.connect() as conn:
        for index, row in source.iterrows():
            pk_values = (row['OrderID'], row['PatientID'],row['AdmissionID'],row['ObjID'], row['SequenceID'])
            
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
        a = [f't.{col} = s.{col}' for col in df.columns if col != key_1 and col != key_2 and col != key_3 and col != key_4 and col != 5]
        temp_table = f'{table_name}_temporary_table'
        
        # Upload the temp table to SQL Server
        df.to_sql(temp_table, schema='staging_rscm', con=conn_staging_sqlserver, if_exists = 'replace', index = False)
        update_stmt = (
            f'UPDATE t SET ' + ", ".join(a) +
            f', t.UpdateDateStaging = CONVERT(DATETIME2(0), GETDATE(),120)'
            f' FROM staging_rscm.{table_name} t '
            f'INNER JOIN (SELECT * FROM staging_rscm.{temp_table}) AS s ON t.{key_1} = s.{key_1} AND t.{key_2} = s.{key_2} AND t.{key_3} = s.{key_3} AND t.{key_4} = s.{key_4} AND t.{key_5} = s.{key_5}'
            f' WHERE t.{key_1} = s.{key_1} AND t.{key_2} = s.{key_2} AND t.{key_3} = s.{key_3} AND t.{key_4} = s.{key_4} AND t.{key_5} = s.{key_5} ;'
        )
        delete_stmt = f'DROP TABLE staging_rscm.{temp_table};'
        print(update_stmt)
        
        with conn_staging_sqlserver.begin() as transaction:
            # Execute update and delete temp table
            conn_staging_sqlserver.execute(update_stmt)
            conn_staging_sqlserver.execute(delete_stmt)
            logging.info("success updated data %s rows", df.shape[0])
    else:
        logging.info("There is no data updated")   

def inserted_data(inserted, conn_staging_sqlserver):
    """ insert data di tabel target """
    if not inserted.empty:
        with conn_staging_sqlserver.begin() as transaction:
            today = dt.datetime.now()
            today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
            inserted['InsertDateStaging'] = today_convert
            inserted.to_sql('TransResultFormUrology', schema='staging_rscm', con=conn_staging_sqlserver, if_exists='append', index=False)
            logging.info("Success inserted new data %s rows ", inserted.shape[0])
    else:
        logging.info("There is no data new")

def send_email(subject, body):
    """Function buat kirim notif ke email kalau error"""

    try:
        logging.info("Preparing to send email notification.")
        
        # Email configuration
        sender_email = db_connection.email_sender # Replace with your email
        receiver_email = db_connection.email_receiver  # Replace with receiver's email
        password = db_connection.email_password  # Replace with your app-specific password

        # Create the email
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = receiver_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))
        
        # Connect to the mail server and send email
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            logging.info("Connecting to SMTP server.")
            server.starttls()
            server.login(sender_email, password)
            server.send_message(msg)
        logging.info("Email sent successfully.")
    except Exception as e:
        logging.error("Failed to send email notification: %s", str(e))

def main():
    """ Function utama untuk menjalankan semua proses"""

    # Get the current process info
    process = psutil.Process(os.getpid())
    
    # Measure memory usage before code execution
    memory_before = process.memory_info().rss / (1024 * 1024)  # In MB
    logging.info("Memory before execution: %s", memory_before)

    # hitung waktu awal sebelum code running
    t0 = time.time()

    try:
        conn_his_live, conn_staging_sqlserver = get_connections()

        # Ambil data dari source
        source = get_source_data(conn_his_live)

        # Menjalankan fungsi translate_same_obj,masukkan parameter obj_id dibawah ini lalu di looping
        list_obj_id = [53835,163039,163040] 
        for obj_id in list_obj_id:
            translate_same_obj(obj_id,conn_his_live, source)
        
        # Menjalankan fungsi translate_different_obj dengan parameter obj_id = 163038
        translate_different_obj(163038,conn_his_live, source)

        # print(source.apply(transform_row,axis=1).to_list())
        source = pd.concat(source.apply(transform_row, axis=1).to_list(), ignore_index=True)

        # Menjalankan fungsi membuat kolom SnomedID 
        source = snomed_id(source)

        print('Ini source fix setelah transform kolom')
        print(source)

        # ambil data dari database target
        target = fetch_target_data(source, conn_staging_sqlserver)
        print(target)

        # Deteksi perubahan (buat dapetin modified dan inserted)
        modified, inserted = detect_changes(source, target)
        logging.info("Total rows data modified: %s", modified.shape[0])
        logging.info("Total rows data inserted: %s", inserted.shape[0])

        # update data
        updated_data(modified, 'TransResultFormUrology', 'OrderID','PatientID','AdmissionID','ObjID','SequenceID', conn_staging_sqlserver)

        # insert data
        inserted_data(inserted, conn_staging_sqlserver)

    except Exception as e:
        # Log the error
        error_message = (
            f"Dear Hanif,\n\n"
            f"There is an error in your script to TransResultFormUrology, here's the error :\n\n"
            f"{str(e)}\n\n"
            f"Please investigate the issue.\n\n"
            f"Best regards,\nYour Monitoring Script"
        )
        logging.error(e)

        # Send Email notification
        send_email(subject='Alert For TransResultFormUrology',body=error_message)

    finally:
        db_connection.close_connection(conn_staging_sqlserver)
        db_connection.close_connection(conn_his_live)

        # Measure memory usage after code execution
        memory_after = process.memory_info().rss / (1024 * 1024)  # In MB
        logging.info("Memory after execution: %s", memory_after)

        # Calculate memory used
        memory_used = memory_after - memory_before
        logging.info("Memory used: %s", memory_used)

        # hitung kecepatan eksekusi program
        t1 = time.time()
        total=t1-t0
        logging.info("total time execution: %s\n", total)

# Run the main process
if __name__ == "__main__":
    main()
        