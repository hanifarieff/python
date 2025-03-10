""" File ini berfungsi untuk menarik data `Hasil Lab Patologi Anatomi` 
di `Menu Input Hasil, Group Admin-Patologi Anatomi` ke *DB Staging*  """

import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pandas as pd
import pyodbc
import numpy as np
import datetime as dt
import time
import psutil
import os
import re
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# log path file error
log_file_path = "C:/TestPython/DWH_SQL_Server/Staging/Log/TransPathologyAnatomy.log"

logging.basicConfig(
    filename = log_file_path,
    level = logging.DEBUG, # Pilih debug untuk capture semua jenis error
    format =  '%(asctime)s - %(levelname)s - %(message)s',
    # filemode="w" # Selalu update log di file yang sama, tidak insert baru
)

def connect_his():
    """ Membuat koneksi ke DB HIS"""
    conn_his_live = db_connection.create_connection(db_connection.his_live)
    logging.info("Success connect to DB HIS LIVE!")
    return conn_his_live

def connect_staging():
    """ Membuat koneksi ke DB Staging"""
    conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)
    logging.info("Success connect to database staging SQLServer!")
    return conn_staging_sqlserver

def get_source_data(conn_his_live):
    """ Extract data dari database sumber (HIS LIVE)""" 
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
                                            WHEN b.obj_id IN (68725) THEN 
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
                                -- a.created_dttm >= '2025-02-01 00:00:00' and a.created_dttm <= '2025-02-12 23:59:59'
                                -- a.no_pa_id = 'H24-02103'
                                ORDER BY a.created_dttm
                        """, conn_his_live)
    
    # Format beberapa kolom yang mengandung \t, \n atau null jadi string kosong atau 0
    source['ObsValue'] = source['ObsValue'].str.replace('\t', ' ').str.replace('\\n','\n')
    source['FinalUserID'] = source['FinalUserID'].replace(np.nan,0).astype('int64')
    source['ObjID'].replace({np.nan:0}, inplace=True)
    
    logging.info("Extracted %s rows from source HIS LIVE", source.shape[0])
    return source

def transform_regex(conn_his_live, source):
    """ Fungsi ini untuk ambil terjemahan value dari obj 55606, lalu hasilnya apply di kolom `[ObsValue]`"""

    # Buat ambil value untuk obj_id = 55606
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

    return source

def get_target_data(source, conn_staging_sqlserver):
    """ Ambil data dari table Target TransPathologyAnatomy"""
    
    # ambil value dari kolom RegistrationNo,OrderID,PatientID,AdmissionID,ObjID untuk dimasukkan ke WHERE query target
    registration_no = tuple(source["RegistrationNo"].unique())
    order_id = tuple(source["OrderID"].unique())
    patient_id = tuple(source["PatientID"].unique())
    admission_id = tuple(source["AdmissionID"].unique())
    obj_id = tuple(source["ObjID"].unique())

    # bikin function remove comma jika ada variable yang isinya cuma 1, variable ini akan di pakai IN CLAUSE di query target
    def remove_comma(x):
        if len(x) == 1:
            return str(x).replace(',','')
        else:
            return x
        
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
    return target

def detect_changes(source,target):
    """ deteksi perubahan antara dataframe `source` dan `target` """

    # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

    # ambil data yang update
    modified = changes[changes[['RegistrationNo','OrderID','PatientID','AdmissionID','ObjID']].apply(tuple,1).isin(target[['RegistrationNo','OrderID','PatientID','AdmissionID','ObjID']].apply(tuple,1))]

    #ambil data yang baru
    inserted = changes[~changes[['RegistrationNo','OrderID','PatientID','AdmissionID','ObjID']].apply(tuple,1).isin(target[['RegistrationNo','OrderID','PatientID','AdmissionID','ObjID']].apply(tuple,1))]

    return modified,inserted

def updated_data(df, table_name,key_1,key_2,key_3,key_4,key_5, conn_staging_sqlserver):
    """ update data di tabel target yaitu TransPathologyAnatomy"""

    if not df.empty:
        a = [f't.{col} = s.{col}' for col in df.columns if col != key_1 or col != key_2 or col != key_3 
             or col != key_4 or col != key_4 or col != key_5 ]
        temp_table =  f'{table_name}_temporary_table'  

        # Upload the temp table to SQL Server
        df.to_sql(temp_table, schema='staging_rscm', con=conn_staging_sqlserver, if_exists = 'replace', index = False)
        update_stmt = (
            f'UPDATE t SET ' +  ", ".join(a) +
            f', t.UpdateDateStaging = CONVERT(DATETIME2(0),GETDATE(),120)'
            f' FROM staging_rscm.{table_name} t '
            f'INNER JOIN (SELECT * FROM staging_rscm.{temp_table}) AS s ON t.{key_1} = s.{key_1} AND t.{key_2} = s.{key_2} AND t.{key_3} = s.{key_3} '
            f' AND t.{key_4} = s.{key_4} AND t.{key_5} = s.{key_5} '
            f'WHERE t.{key_1} = s.{key_1} AND t.{key_2} = s.{key_2} AND t.{key_3} = s.{key_3} AND t.{key_4} = s.{key_4} AND t.{key_5} = s.{key_5};'
        )

        delete_stmt = f'DROP TABLE staging_rscm.{temp_table} '
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
            inserted.to_sql('TransPathologyAnatomy', schema='staging_rscm', con=conn_staging_sqlserver, if_exists='append', index=False)
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
        # connect DB HIS Live
        conn_his_live = connect_his()

        # ambil data dari source 
        source = get_source_data(conn_his_live)
        print(source)

        # jalankan fungsi transform_regex
        transform_regex(conn_his_live,source)
        
        # connect DB Staging
        conn_staging_sqlserver = connect_staging()

        # ambil data dari target
        target = get_target_data(source,conn_staging_sqlserver)

        # Deteksi perubahan (buat dapetin modified dan inserted)
        modified, inserted = detect_changes(source, target)
        logging.info("Total rows data modified: %s", modified.shape[0])
        logging.info("Total rows data inserted: %s", inserted.shape[0])

        # update data
        updated_data(modified, 'TransPathologyAnatomy', 'RegistrationNo','OrderID','PatientID','AdmissionID','ObjID', conn_staging_sqlserver)

        # insert new data
        inserted_data(inserted, conn_staging_sqlserver)
   
    except Exception as  e:
        # Log the error
        error_message = (
            f"Dear Hanif,\n\n"
            f"There is an error in your script to TransPathologyAnatomy, here's the error :\n\n"
            f"{str(e)}\n\n"
            f"Please investigate the issue.\n\n"
            f"Best regards,\nYour Monitoring Script"
        )
        logging.error(e)

        # Send Email notification
        send_email(subject='Alert For TransPathologyAnatomy',body=error_message)

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
        