""" File ini bertujuan untuk menarik data dari `TransPrescriptionResponsTime` ke `DWH`"""

import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pyodbc
import pandas as pd
import time
import datetime as dt
import os
import psutil
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# log path file error
log_file_path = "C:/TestPython/DWH_SQL_Server/DWH/Log/FactPrescriptionResponsTime.log"

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
                            SELECT  DISTINCT
                                TRIM(a.PrescriptionID) AS PrescriptionID,
                                a.PatientID,
                                a.AdmissionID,
                                c.MedicalNo,
                                UPPER(c.PatientName) AS PatientName,
                                a.AdmissionDate,
                                a.OrgID,
                                CASE	
                                    WHEN d.AppointmentID != 0 THEN 'ONLINE'
                                    ELSE 'OFFLINE'
                                END AS RegistrationType,
                                a.ResponTimeID,
                                a.ResponTimeType,
                                a.ResponTimeName,
                                a.ItemAmountNonRacikan,
                                a.ItemAmountRacikan,
                                a.OrderDate,
                                a.DispenseDate,
                                a.PreparedDate,
                                a.CheckedDate,
                                a.FinishedDate,
                                a.GivenDate,
                                CASE
                                    WHEN disp.EmployeeName IS NULL THEN '-'
                                    ELSE disp.EmployeeName
                                END AS DispenseUser,
                                CASE
                                    WHEN prep.EmployeeName IS NULL THEN '-'
                                    ELSE prep.EmployeeName
                                END as PreparedUser,
                                CASE
                                    WHEN che.EmployeeName IS NULL THEN '-'
                                    ELSE che.EmployeeName
                                END AS CheckedUser,
                                CASE
                                    WHEN fin.EmployeeName IS NULL THEN '-'
                                    ELSE fin.EmployeeName
                                END AS FinishedUser,
                                CASE
                                    WHEN giv.EmployeeName IS NULL THEN '-'
                                    ELSE giv.EmployeeName
                                END AS GivenUser,
                                a.TransactionTime
                            from staging_rscm.TransPrescriptionResponsTime a
                            left join staging_rscm.DimensionPatientMPI c on a.PatientID = c.PatientID and c.ScdActive = 1
                            left join staging_rscm.TransAppointmentQueue d on a.PatientID = d.PatientID and a.AdmissionID = d.AdmissionID and d.AppointmentID != 0
                            left join staging_rscm.DimensionEmployee disp on a.DispenseUser = disp.UserID and disp.SCDActive = 1
                            left join staging_rscm.DimensionEmployee prep  on a.PreparedUser = prep.UserID and prep.SCDActive = 1
                            left join staging_rscm.DimensionEmployee che on a.CheckedUser = che.UserID and che.SCDActive = 1
                            left join staging_rscm.DimensionEmployee fin on a.FinishedUser = fin.UserID and fin.SCDActive = 1
                            left join staging_rscm.DimensionEmployee giv on a.GivenUser = giv.UserID and giv.SCDActive = 1
                            WHERE
                            CAST(a.InsertDateStaging as date) >= CAST(DATEADD(DAY, -2, GETDATE()) as date) AND CAST(a.InsertDateStaging as date) <= CAST(GETDATE() as date) 
                            OR CAST(a.UpdateDateStaging as date) >= CAST(DATEADD(DAY, -1, GETDATE()) as date) AND CAST(a.UpdateDateStaging as date) <= CAST(GETDATE() as date)
                            -- DispenseDate >= '2024-09- 00:00:00' and DispenseDate <= '2024-09-24 23:59:59'
                           """, conn_staging_sqlserver)
    
    logging.info("Extracted %s rows from DB Staging", source.shape[0])
    return source

def get_target_data(source, conn_dwh_sqlserver):
    """ Ambil data dari target tabel TransPrescriptionResponsTime """
    prescriptionid = tuple(source["PrescriptionID"].unique())

    # bikin function remove comma jika ada variable yang isinya cuma 1, variable ini akan di pakai IN CLAUSE di query target
    def remove_comma(x):
        if len(x) == 1:
            return str(x).replace(',','')
        else:
            return x
        
    prescriptionid = remove_comma(prescriptionid)
    
    # query buat narik data dari target lalu filter berdasarkan primary key
    query = f""" SELECT 
                        TRIM(PrescriptionID) PrescriptionID,
                        PatientID,
                        AdmissionID,
                        MedicalNo,
                        PatientName,
                        AdmissionDate,
                        OrgID,
                        RegistrationType,
                        ResponTimeID,
                        ResponTimeType,
                        ResponTimeName,
                        ItemAmountNonRacikan,
                        ItemAmountRacikan,
                        OrderDate,
                        DispenseDate,
                        PreparedDate,
                        CheckedDate,
                        FinishedDate,
                        GivenDate,
                        DispenseUser,
                        PreparedUser,
                        CheckedUser,
                        FinishedUser,
                        GivenUser,
                        CAST(TransactionTime as varchar(8)) as TransactionTime
                    FROM dwhrscm_talend.FactPrescriptionResponsTime 
                    WHERE PrescriptionID IN {prescriptionid} ORDER BY PrescriptionID"""
    target = pd.read_sql_query(query, conn_dwh_sqlserver)
    return target

def detect_changes(source, target):
    """ deteksi perubahan antara dataframe `source` dan `target` """
    
    # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

    # ambil data yang update
    modified = changes[changes[['PrescriptionID']].apply(tuple,1).isin(target[['PrescriptionID']].apply(tuple,1))]

    # ambil data yang baru
    inserted = changes[~changes[['PrescriptionID']].apply(tuple,1).isin(target[['PrescriptionID']].apply(tuple,1))]

    return modified,inserted

def updated_data(df, table_name, key_1, conn_dwh_sqlserver):
    """ update data di tabel target """
    if not df.empty:
        a = [f't.{col} = s.{col}' for col in df.columns if col != key_1]
        temp_table = f'{table_name}_temporary_table'
        
        # Upload the temp table to SQL Server
        df.to_sql(temp_table, schema='dwhrscm_talend', con=conn_dwh_sqlserver, if_exists = 'replace', index = False)
        update_stmt = (
            f'UPDATE t SET ' + ", ".join(a) +
            f', t.UpdateDateDWH = CONVERT(DATETIME2(0), GETDATE(),120) '
            f' FROM dwhrscm_talend.{table_name} t '
            f'INNER JOIN (SELECT * FROM dwhrscm_talend.{temp_table}) AS s ON t.{key_1} = s.{key_1} '
            f'WHERE t.{key_1} = s.{key_1};'
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
            inserted.to_sql('FactPrescriptionResponsTime', schema='dwhrscm_talend', con=conn_dwh_sqlserver, if_exists='append', index=False)
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
            updated_data(modified, 'FactPrescriptionResponsTime', 'PrescriptionID', conn_dwh_sqlserver)

            # insert data
            inserted_data(inserted, conn_dwh_sqlserver)
    
    except Exception as e:
         # Log the error
        error_message = (
            f"Dear Hanif,\n\n"
            f"There is an error in your script to FactPrescriptionResponsTime, here's the error :\n\n"
            f"{str(e)}\n\n"
            f"Please investigate the issue.\n\n"
            f"Best regards,\nYour Monitoring Script"
        )
        logging.error(e)

        # Send Email notification
        send_email(subject='Alert For FactPrescriptionResponsTime',body=error_message)

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
        
