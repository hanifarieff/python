""" File ini berfungsi untuk menarik data `DimensionPatientMPI`ke `DWH` """

import sys
import os
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import datetime as dt
date = dt.datetime.today()
import time
import pandas as pd
import pyodbc
import psutil
import time
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# log path file error
log_file_path = "C:/TestPython/DWH_SQL_Server/DWH/Log/DimPatientMPI.log"

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
    """ Extract data MPI dari database staging `DimensionPatientMPI` """ 
    # ambil source
    source = pd.read_sql_query(""" SELECT 
                                    PatientSurrogateKey,
                                    PatientID,
                                    PersonID,
                                    TRIM(Confidentiality) AS Confidentiality,
                                    PatientStatus,
                                    MigrationID,
                                    MedicalNo,
                                    PatientName,
                                    BirthDate,
                                    TRIM(Gender) AS Gender,
                                    MaritalStatus,
                                    ReligionID,
                                    NIK,
                                    Address,
                                    PatientCreatedDate,
                                    PatientUpdatedDate,
                                    PatientNullifiedDate,
                                    RegionalCode,
                                    PersonStatusCode,
                                    PatientStatusCode,
                                    PersonTitle,
                                    FamilyName,
                                    ExternalID,
                                    PassportNo,
                                    FatherName,
                                    MotherName,
                                    SpouseName,
                                    PlaceOfBirth,
                                    TRIM(PostalCode) AS PostalCode,
                                    Race,
                                    Education,
                                    Occupation,
                                    BloodType,
                                    Nationality,
                                    TelephoneNo,
                                    PhoneNo,
                                    Email,
                                    ScdActive,
                                    ScdStart,
                                    ScdEnd,
                                    EmployeeID,
                                    IsEmployee,
                                    InsertedDateDWH
                                FROM staging_rscm.DimensionPatientMPI
                                WHERE
                                -- PatientID IN (1657653) 
                                -- PatientCreatedDate >= '2023-03-11 00:00:00' and PatientCreatedDate <= '2023-03-15 23:59:59'  
                                -- (CAST(InsertedDateDWH as date) >= '2024-04-13' AND CAST(InsertedDateDWH as date) <= '2024-04-16')
                                -- OR (CAST(ScdEnd as date) >= '2024-04-13' AND CAST(ScdEnd as date) <= '2024-04-16')
                                (CAST(InsertedDateDWH as date) >= CAST(DATEADD(DAY, -1, GETDATE()) as date) AND CAST(InsertedDateDWH as date) <= CAST(GETDATE() as date))
                                OR (CAST(ScdEnd as date) >= CAST(DATEADD(DAY, -1, GETDATE()) as date) AND CAST(ScdEnd as date) <= CAST(GETDATE() as date))
                                -- WHERE CAST(ScdStart as date) = CAST(GETDATE() as date)
        """, conn_staging_sqlserver)
    source['BirthDate'] = pd.to_datetime(source['BirthDate'], format="%Y-%m-%d %H:%M:%S", errors='coerce').dt.strftime("%Y-%m-%d %H:%M:%S")
    source['PatientCreatedDate'] = pd.to_datetime(source['PatientCreatedDate'], format="%Y-%m-%d %H:%M:%S", errors='coerce').dt.strftime("%Y-%m-%d %H:%M:%S")
    source['PatientUpdatedDate'] = pd.to_datetime(source['PatientUpdatedDate'], format="%Y-%m-%d %H:%M:%S")
    source['PatientNullifiedDate'] = pd.to_datetime(source['PatientNullifiedDate'], format="%Y-%m-%d %H:%M:%S")
    source['ScdStart'] = pd.to_datetime(source['ScdStart'], format="%Y-%m-%d %H:%M:%S")
    source['ScdEnd'] = pd.to_datetime(source['ScdEnd'], format="%Y-%m-%d %H:%M:%S")
    source['InsertedDateDWH'] = pd.to_datetime(source['InsertedDateDWH'], format="%Y-%m-%d %H:%M:%S")

    logging.info("Extracted %s rows from DB Staging", source.shape[0])
    return source

def get_target_data(source,conn_dwh_sqlserver):
    # ambil primary key dari source, pake unique biar tidak duplicate
    patientsurrogatekey = tuple(source["PatientSurrogateKey"].unique())

    # bikin function remove comma jika ada variable yang isinya cuma 1, variable ini akan di pakai IN CLAUSE di query target
    def remove_comma(x):
        if len(x) == 1:
            return str(x).replace(',','')
        else:
            return x

    patientsurrogatekey = remove_comma(patientsurrogatekey)
    
    # query buat narik data dari target lalu filter berdasarkan primary key
    query = f""" 
                SELECT 
                    PatientSurrogateKey,
                    PatientID,
                    PersonID,
                    TRIM(Confidentiality) AS Confidentiality,
                    PatientStatus,
                    MigrationID,
                    MedicalNo,
                    PatientName,
                    BirthDate,
                    TRIM(Gender) AS Gender,
                    MaritalStatus,
                    ReligionID,
                    NIK,
                    Address,
                    PatientCreatedDate,
                    PatientUpdatedDate,
                    PatientNullifiedDate,
                    RegionalCode,
                    PersonStatusCode,
                    PatientStatusCode,
                    PersonTitle,
                    FamilyName,
                    ExternalID,
                    PassportNo,
                    FatherName,
                    MotherName,
                    SpouseName,
                    PlaceOfBirth,
                    TRIM(PostalCode) AS PostalCode,
                    Race,
                    Education,
                    Occupation,
                    BloodType,
                    Nationality,
                    TelephoneNo,
                    PhoneNo,
                    Email,
                    ScdActive,
                    ScdStart,
                    ScdEnd,
                    EmployeeID,
                    IsEmployee,
                    InsertedDateDWH
                FROM dwhrscm_talend.DimPatientMPI 
                WHERE PatientSurrogateKey IN {patientsurrogatekey}
                ORDER BY PatientSurrogateKey """
    target = pd.read_sql_query(query, conn_dwh_sqlserver)
    return target

def detect_changes(source, target):
    """ deteksi perubahan antara dataframe `source` dan `target` """
    
    # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

    # ambil data yang update
    modified = changes[changes[['PatientSurrogateKey']].apply(tuple,1).isin(target[['PatientSurrogateKey']].apply(tuple,1))]

    # ambil data yang baru
    inserted = changes[~changes[['PatientSurrogateKey']].apply(tuple,1).isin(target[['PatientSurrogateKey']].apply(tuple,1))]

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
            inserted.to_sql('DimPatientMPI', schema='dwhrscm_talend', con=conn_dwh_sqlserver, if_exists='append', index=False)
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
        logging.info("Email sent successfully.\n")
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
            logging.info("Here's sample data modified:\n%s", modified[['PatientID']].head(2).to_string(index=False))
            logging.info("Total rows data inserted: %s", inserted.shape[0])
            logging.info("Here's sample data inserted:\n%s", inserted[['PatientID']].head(2).to_string(index=False))


            # update data
            updated_data(modified, 'DimPatientMPI', 'PatientSurrogateKey', conn_dwh_sqlserver)

            # insert data
            inserted_data(inserted, conn_dwh_sqlserver)
    
    except Exception as e:
         # Log the error
        error_message = (
            f"Dear Hanif,\n\n"
            f"There is an error in your script to DimPatientMPI, here's the error :\n\n"
            f"{str(e)}\n\n"
            f"Please investigate the issue.\n\n"
            f"Best regards,\nYour Monitoring Script"
        )
        logging.error(e)

        # Send Email notification
        send_email(subject='Alert For DimPatientMPI',body=error_message)

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
