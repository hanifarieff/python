""" File ini berfungsi untuk menarik data dari tabel `TransResultFormRadiotherapy` ke `DWH` """
import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pandas as pd
import pyodbc
import numpy as np
import datetime as dt
date = dt.datetime.today()
import time
import os
import psutil
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


# log path file error
log_file_path = "C:/TestPython/DWH_SQL_Server/DWH/Log/FactResultFormRadiotherapy.log"

logging.basicConfig(
    filename = log_file_path,
    level = logging.DEBUG, # Pilih debuh untuk capture semua jenis error
    format =  '%(asctime)s - %(levelname)s - %(message)s',
    # filemode="w" # Selalu update log di file yang sama, tidak insert baru
)

def get_connections():
    """ Membuat koneksi ke database Staging dan DWH"""
    conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)
    conn_dwh_sqlserver = db_connection.create_connection(db_connection.dwh_sqlserver)
    logging.info("success connect to DB!")
    return conn_staging_sqlserver, conn_dwh_sqlserver

def get_source_data(conn_staging_sqlserver):
    """ Extract data dari database staging """ 
    source = pd.read_sql_query("""                                  
                            SELECT 
                                a.OrderID,
                                a.PatientID,
                                a.AdmissionID,
                                b.MedicalNo,
                                a.ObservationDate,
                                a.ObjID,
                                a.SequenceID,
                                a.ObjName,
                                a.Checkup,
                                a.CheckupDate,
                                a.Result,
                                a.SatuSehatCancerResult,
                                a.StatusForm,
                                a.CreatedUserID
                            FROM staging_rscm.TransResultFormRadiotherapy a
                            LEFT JOIN staging_rscm.DimensionPatientMPI b on a.PatientID = b.PatientID and b.ScdActive ='1'
                            WHERE 
                            -- a.OrderID = 157874408
                            (CAST(a.InsertDateStaging as date) >= CAST(DATEADD(DAY, -2, GETDATE()) as date) AND CAST(a.InsertDateStaging as date) <= CAST(GETDATE() as date) 
                            OR CAST(a.UpdateDateStaging as date) >= CAST(DATEADD(DAY, -2, GETDATE()) as date) AND CAST(a.UpdateDateStaging as date) <= CAST(GETDATE() as date)) 
                            -- a.CreatedDate >= '2024-07-21 00:00:00' and a.CreatedDate <= '2024-07-30 23:59:59'
                            AND (MedicalNo NOT IN (SELECT MedicalNo FROM staging_rscm.DimensionDummyPatient) OR MedicalNo IS NULL)
                    """, conn_staging_sqlserver)
    logging.info("Extracted %s rows from DB Staging", source.shape[0])
    return source

def get_target_data(source, conn_dwh_sqlserver):
    """  Ambil data dari target yaitu TransResultFormRadiotherapy"""
    order_id = tuple(source["OrderID"].unique())
    patient_id = tuple(source["PatientID"].unique())
    admission_id = tuple(source["AdmissionID"].unique())
    obj_id = tuple(source["ObjID"].unique())
    sequence_id = tuple(source['SequenceID'].unique())

    # bikin function remove comma jika ada variable yang isinya cuma 1, variable ini akan di pakai IN CLAUSE di query target
    def remove_comma(x):
        if len(x) == 1:
            return str(x).replace(',','')
        else:
            return x
    
    order_id = remove_comma(order_id)
    patient_id = remove_comma(patient_id)
    admission_id = remove_comma(admission_id)
    obj_id = remove_comma(obj_id)
    sequence_id = remove_comma(sequence_id)

    # query buat narik data dari target lalu filter berdasarkan primary key
    query = f"""SELECT     
                    OrderID, 
                    PatientID, 
                    AdmissionID, 
                    MedicalNo,
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
                FROM dwhrscm_talend.FactResultFormRadiotherapy WHERE OrderID IN {order_id} AND PatientID IN {patient_id} 
                AND AdmissionID IN {admission_id} AND ObjID IN {obj_id} AND SequenceID in {sequence_id}
            """
    target = pd.read_sql_query(query, conn_dwh_sqlserver)
    return target

def detect_changes(source, target):
    """ deteksi perubahan antara dataframe `source` dan `target` """
    
    # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

    # ambil data yang update
    modified = changes[changes[['OrderID','PatientID','AdmissionID','ObjID','SequenceID']].apply(tuple,1).isin(target[['OrderID','PatientID','AdmissionID','ObjID','SequenceID']].apply(tuple,1))]

    # ambil data yang baru
    inserted = changes[~changes[['OrderID','PatientID','AdmissionID','ObjID','SequenceID']].apply(tuple,1).isin(target[['OrderID','PatientID','AdmissionID','ObjID','SequenceID']].apply(tuple,1))]

    return modified,inserted

def updated_data(df, table_name, key_1,key_2,key_3,key_4,key_5, conn_dwh_sqlserver):
    """ update data di tabel target """
    if not df.empty:
        a = [f't.{col} = s.{col}' for col in df.columns if col != key_1 and col != key_2 and col != key_3 and col != key_4 and col != 5]
        temp_table = f'{table_name}_temporary_table'
        
        # Upload the temp table to SQL Server
        df.to_sql(temp_table, schema='dwhrscm_talend', con=conn_dwh_sqlserver, if_exists = 'replace', index = False)
        update_stmt = (
            f'UPDATE t SET ' + ", ".join(a) +
            f', t.UpdatedDateDWH = CONVERT(DATETIME2(0), GETDATE(),120)'
            f' FROM dwhrscm_talend.{table_name} t '
            f'INNER JOIN (SELECT * FROM dwhrscm_talend.{temp_table}) AS s ON t.{key_1} = s.{key_1} AND t.{key_2} = s.{key_2} AND t.{key_3} = s.{key_3} AND t.{key_4} = s.{key_4} AND t.{key_5} = s.{key_5}'
            f' WHERE t.{key_1} = s.{key_1} AND t.{key_2} = s.{key_2} AND t.{key_3} = s.{key_3} AND t.{key_4} = s.{key_4} AND t.{key_5} = s.{key_5} ;'
        )
        delete_stmt = f'DROP TABLE dwhrscm_talend.{temp_table};'
        
        with conn_dwh_sqlserver.begin() as transaction:
            # Execute update and delete temp table
            conn_dwh_sqlserver.execute(update_stmt)
            conn_dwh_sqlserver.execute(delete_stmt)
            logging.info("success updated data %s rows", df.shape[0])
    else:
        logging.info("There is no data updated")   

def inserted_data(inserted, conn_dwh_sqlserver):
    """ insert data di tabel target """
    if not inserted.empty:
        with conn_dwh_sqlserver.begin() as transaction:
            today = dt.datetime.now()
            today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
            inserted['InsertedDateDWH'] = today_convert
            inserted.to_sql('FactResultFormRadiotherapy', schema='dwhrscm_talend', con=conn_dwh_sqlserver, if_exists='append', index=False)
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
        # koneksi ke database
        conn_staging_sqlserver,conn_dwh_sqlserver = get_connections()

        # Ambil data dari source
        source = get_source_data(conn_staging_sqlserver)
        print(source)       
        
        if source.empty:
            logging.info("There is no data updated and inserted from staging!")
        else:
            # ambil data dari database target
            target = get_target_data(source, conn_dwh_sqlserver)
            print(target)

            # Deteksi perubahan (buat dapetin modified dan inserted)
            modified, inserted = detect_changes(source, target)
            logging.info("Total rows data modified: %s", modified.shape[0])
            logging.info("Total rows data inserted: %s", inserted.shape[0])

            # update data
            updated_data(modified, 'FactResultFormRadiotherapy', 'OrderID','PatientID','AdmissionID','ObjID','SequenceID', conn_dwh_sqlserver)

            # insert data
            inserted_data(inserted, conn_dwh_sqlserver)
    
    except Exception as e:
         # Log the error
        error_message = (
            f"Dear Hanif,\n\n"
            f"There is an error in your script to FactResultFormRadiotherapy, here's the error :\n\n"
            f"{str(e)}\n\n"
            f"Please investigate the issue.\n\n"
            f"Best regards,\nYour Monitoring Script"
        )
        logging.error(e)

        # Send Email notification
        send_email(subject='Alert For FactResultFormRadiotherapy',body=error_message)

    finally:
        db_connection.close_connection(conn_staging_sqlserver)
        db_connection.close_connection(conn_dwh_sqlserver)

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
        
