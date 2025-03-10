""" File ini berfungsi untuk menarik data `Master Pasien` dari DB MPI ke DB Staging """
import psutil
import os
import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pandas as pd
import numpy as np
import pyodbc
import datetime as dt
date = dt.datetime.today()
import time
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# log path file error
log_file_path = "C:/TestPython/DWH_SQL_Server/Staging/Log/DimensionPatientMPI.log"

# Setup logging to capture all levels
logging.basicConfig(
    filename=log_file_path,
    level=logging.DEBUG,  # Pilih debug untuk capture semua jenis error
    format='%(asctime)s - %(levelname)s - %(message)s',
    # filemode="w" # rewrite the log everyday
)

def connect_mpi():
    """ Membuat koneksi ke DB MPI """
    conn_mpi  = db_connection.create_connection(db_connection.mpi)
    logging.info("Success connect ke DB MPI")
    return conn_mpi
def connect_ehr():    
    """ Membuat koneksi ke DB EHR """
    conn_ehr = db_connection.create_connection(db_connection.replika_ehr)
    logging.info("Success connect ke DB EHR")
    return conn_ehr
def connect_staging():
    """ Membuat koneksi ke DB Staging SQL Server """
    conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)
    logging.info("Success connect ke DB Staging")
    return conn_staging_sqlserver

def get_source_mpi(conn_mpi):
    """ Extract Data Patient dari DB MPI"""

    # tarik data berdasarkan tanggal create tabel patients
    source1 = pd.read_sql_query(""" SELECT 
                                    pt.patient_id as PatientID,
                                    pt.person_id as PersonID,
                                    TRIM(pt.confidentiality_cd) as Confidentiality,
                                    pt.status_cd as PatientStatus,
                                    pt.migration_id as MigrationID,
                                    pt.mrn as MedicalNo, 
                                    pr.person_nm as PatientName , 
                                    pr.date_of_birth as BirthDate,
                                    ifnull(pr.gender_cd,'') as Gender, 
                                    ifnull(pr.marital_cd,'') as MaritalStatus ,  
                                    pr.religion_id as  ReligionID,  
                                    pr.external_id as NIK, 
                                    pr.address_txt  as Address, 
                                    pt.created_dttm as PatientCreatedDate, 
                                    pt.updated_dttm as PatientUpdatedDate, 
                                    pt.nullified_dttm as PatientNullifiedDate, 
                                    case when pr.regional_cd ='' then '-'  else pr.regional_cd end as RegionalCode,
                                    pr.status_cd as PersonStatusCode,
                                    pt.status_cd as PatientStatusCode,
                                    -- pr.updated_dttm as UpdatedDate
                                    -- ConvertMyDate(pr.updated_dttm) as PersonUpdatedDate,
                                    person_title as PersonTitle,
                                    family_nm as FamilyName,
                                    external_id as ExternalID,
                                    passport_no as PassportNo,
                                    father_nm as FatherName,
                                    mother_nm as MotherName,
                                    spouse_nm as SpouseName,
                                    place_of_birth as PlaceOfBirth,
                                    TRIM(postal_cd) as PostalCode,
                                    pr.race as Race,
                                    edu.educlvl_nm as Education,
                                    pr.jobclass_id,
                                    pr.blood_type as BloodType,
                                    pr.nationality as Nationality,
                                    pr.phone1 as TelephoneNo,
                                    pr.phone3 as PhoneNo,
                                    pr.email as Email
                                FROM  patients pt left join persons pr on pt.person_id = pr.person_id 
                                LEFT JOIN hris_sys_cd_educlvl edu ON edu.educlvl_cd = pr.education_id
                                WHERE 
                                -- pt.patient_id IN (2255278,2256017)
                                -- (pt.created_dttm >= '2025-03-02 00:00:00' and pt.created_dttm <= '2025-03-02 23:59:59')
                                -- (pr.updated_dttm >= '2023-10-12 00:00:00' and pr.updated_dttm <= '2023-10-12 23:59:59')
                                (pt.created_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 2 DAY), "%%Y-%%m-%%d 00:00:00") 
                                AND pt.created_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 23:59:59")) 
                                -- AND pr.person_nm IS NOT NULL""", conn_mpi)

    # tarik data yang update berdasarkan kolom update di tabel person
    source2 = pd.read_sql_query(""" SELECT 
                                    pt.patient_id as PatientID,
                                    pt.person_id as PersonID,
                                    TRIM(pt.confidentiality_cd) as Confidentiality,
                                    pt.status_cd as PatientStatus,
                                    pt.migration_id as MigrationID,
                                    pt.mrn as MedicalNo, 
                                    pr.person_nm as PatientName , 
                                    pr.date_of_birth as BirthDate,
                                    ifnull(pr.gender_cd,'') as Gender, 
                                    ifnull(pr.marital_cd,'') as MaritalStatus ,  
                                    pr.religion_id as  ReligionID,  
                                    pr.external_id as NIK, 
                                    pr.address_txt  as Address, 
                                    pt.created_dttm as PatientCreatedDate, 
                                    pt.updated_dttm as PatientUpdatedDate, 
                                    pt.nullified_dttm as PatientNullifiedDate, 
                                    case when pr.regional_cd ='' then '-'  else pr.regional_cd end as RegionalCode,
                                    pr.status_cd as PersonStatusCode,
                                    pt.status_cd as PatientStatusCode,
                                    -- pr.updated_dttm as UpdatedDate
                                    -- ConvertMyDate(pr.updated_dttm) as PersonUpdatedDate,
                                    person_title as PersonTitle,
                                    family_nm as FamilyName,
                                    external_id as ExternalID,
                                    passport_no as PassportNo,
                                    father_nm as FatherName,
                                    mother_nm as MotherName,
                                    spouse_nm as SpouseName,
                                    place_of_birth as PlaceOfBirth,
                                    TRIM(postal_cd) as PostalCode,
                                    pr.race as Race,
                                    edu.educlvl_nm as Education,
                                    pr.jobclass_id,
                                    pr.blood_type as BloodType,
                                    pr.nationality as Nationality,
                                    pr.phone1 as TelephoneNo,
                                    pr.phone3 as PhoneNo,
                                    pr.email as Email
                                FROM  patients pt left join persons pr on pt.person_id = pr.person_id 
                                LEFT JOIN hris_sys_cd_educlvl edu ON edu.educlvl_cd = pr.education_id
                                WHERE 
                                -- pt.patient_id IN (2061500,2061501)
                                -- pr.updated_dttm >= '2025-01-31 00:00:00' and pr.updated_dttm <= '2025-01-31 06:59:59'
                                (pr.updated_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 00:00:00") 
                                and pr.updated_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 23:59:59"))
                                -- AND pr.person_nm IS NOT NULL""", conn_mpi)

    # gabungkan source1 dan source2, lalu pisahkan data yang duplikat berdasarkan PatientID
    source = pd.concat([source1, source2], ignore_index=True)
    # source= source1
    source = source.drop_duplicates(subset='PatientID')
    return source

def get_source_ehr(conn_ehr):
    """ Extract data dari tabel `xocp_ehr_employee_admission` dan `xocp_sys_cd_jobclass` dari DB EHR """ 
    employee_admission = pd.read_sql_query(""" SELECT patient_id as PatientID, employee_id EmployeeID FROM xocp_ehr_employee_admission""", conn_ehr)
    # print(source)

    # join ke tabel ini di ehr buat dapetin pekerjaan pasien
    occupation_patient =  pd.read_sql_query(""" SELECT jobclass_cd as jobclass_id, jobclass_nm AS Occupation FROM xocp_sys_cd_jobclass""",conn_ehr)
    return employee_admission, occupation_patient

def combine_mpi_ehr(source,employee_admission,occupation_patient):
    """ Join antara data dari MPI dan EHR, lalu buat flag apakah Pasien merupakan pegawai RSCM atau tidak"""
    source = source.merge(employee_admission,how='left',on='PatientID').merge(occupation_patient,how='left',on='jobclass_id')

    new_order_columns = ['PatientID','PersonID','Confidentiality','PatientStatus','MigrationID','MedicalNo','PatientName','BirthDate',
                        'Gender','MaritalStatus','ReligionID','NIK','Address','PatientCreatedDate','PatientUpdatedDate',
                        'PatientNullifiedDate','RegionalCode','PersonStatusCode','PatientStatusCode','PersonTitle','FamilyName','ExternalID',
                        'PassportNo','FatherName','MotherName','SpouseName','PlaceOfBirth','PostalCode','Race','Education','Occupation',
                        'BloodType','Nationality','TelephoneNo','PhoneNo','Email','EmployeeID','IsEmployee']
    source = source.reindex(columns=new_order_columns)

    #bikin kondisi untuk kolom IsEmployee kalo EmployeeID null = 0, tidak null = 1
    source.loc[source['EmployeeID'].isnull(),'IsEmployee']=0
    source.loc[source['EmployeeID'].notnull(),'IsEmployee']=1

    # rubah tipe data EmployeeID dan IsEmployee jadi integer
    source['EmployeeID'] = source['EmployeeID'].fillna(0).astype('int64')
    source['IsEmployee'] = source['IsEmployee'].fillna(0).astype('int64')
    source['Occupation'].fillna('-',inplace=True)
    logging.info("success combine with EHR")
    
    return source

def get_target_data(source,conn_staging_sqlserver):
    """ Ambil data dari tabel target, yaitu DimensionPatientMPI """
    logging.info('Process load to Temporary Table')
    source.to_sql('DimensionPatientMPITemporary', schema='staging_rscm',con = conn_staging_sqlserver, if_exists = 'append', index=False)
    query_filter = f""" 
                    SELECT 
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
                        EmployeeID,
                        IsEmployee
                    FROM staging_rscm.DimensionPatientMPI 
                    WHERE PatientID IN (SELECT PatientID FROM staging_rscm.DimensionPatientMPITemporary) 
                    AND ScdActive = 1
                    ORDER BY PatientID """
    target = pd.read_sql_query(query_filter,conn_staging_sqlserver)

    query_drop_table = f'DELETE FROM staging_rscm.DimensionPatientMPITemporary'
    conn_staging_sqlserver.execute(query_drop_table)
    return target

def detect_changes(source, target):
    """ Deteksi perubahan antara dataframe `source` dan `target` """
    
    # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
    change = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

    # ambil data dari variable change ambil primary key PatientID cek dengan variable target, masukkan ke variable modified
    modified = change[change[['PatientID']].apply(tuple,1).isin(target[['PatientID']].apply(tuple,1))]
    
    # tambahkan kolom ScdStart isi dengan tanggal, dan kolom ScdActive beri nilai 1
    modified['ScdStart']=date
    modified['ScdActive']=1

    # ambil data dari variable change, ambil primary key PatientID dan variable target, masukkan ke variable inserted, kebalikan dari update pake simbol ~
    inserted =  change[~change[['PatientID']].apply(tuple,1).isin(target[['PatientID']].apply(tuple,1))]
    inserted['ScdStart']=date
    inserted['ScdActive']=1

    return modified, inserted

def create_surrogate_key(df, conn_staging_sqlserver):
    """ Function untuk Membuat *Surrogate Key*"""
    new_query = f'SELECT PatientSurrogateKey FROM staging_rscm.DimensionPatientMPI '
    new_target = pd.read_sql_query(new_query, conn_staging_sqlserver)
    df.insert(0, 'PatientSurrogateKey', range(len(new_target)+1, len(new_target)+1 + len(df)))
    df.to_sql('DimensionPatientMPI',schema='staging_rscm',con=conn_staging_sqlserver,if_exists='append',index=False)

def update_and_insert_data(target,modified,inserted,conn_staging_sqlserver):
    """ Function untuk melakukan update dan insert data pasien ke tabel """

    #jika target kosong, maka semua data dari source langsung masuk ke table tujuan
    if target.empty:
        create_surrogate_key(inserted,conn_staging_sqlserver)
        # inserted.insert(0, 'PatientSurrogateKey', range(len(target)+1, len(target)+1 + len(inserted)))
        # inserted.to_sql('tes_patient_target',schema='dbo',con=conn_mssql,if_exists='append',index=False)
        print('all data success inserted')
        logging.info("success inserted %s rows without data updated!", inserted.shape[0])

    else:
        try:
            #jika tidak ada data yang update, data yg new record langsung masuk ke table tujuan
            if modified.empty:
                if inserted.empty:
                    print('there is no new and updated data')
                    logging.info("there is no new and updated data")
                else:
                    create_surrogate_key(inserted,conn_staging_sqlserver)
                    print('success add new record without any update')
                    logging.info("success add new record %s rows without any update",inserted.shape[0])
            else:
                if inserted.empty:
                    #jika tidak ada data yang baru, maka jalankan surrogate_key data yang modified saja
                    if len(modified['PatientID']) == 1:
                        patientid_update = modified['PatientID'].values[0]
                        query_update = f" UPDATE staging_rscm.DimensionPatientMPI SET ScdEnd = '{date}', ScdActive = 0 WHERE PatientID in ({patientid_update}) and ScdActive =1"
                        conn_staging_sqlserver.execute(query_update)
                        print('success update data existing')
                        logging.info("success update data existing")

                    else :
                        patientid_update = tuple(modified['PatientID'])
                        query_update = f"UPDATE staging_rscm.DimensionPatientMPI SET ScdEnd = '{date}', ScdActive = 0 WHERE PatientID in {patientid_update} and ScdActive =1"
                        conn_staging_sqlserver.execute(query_update)

                    create_surrogate_key(modified,conn_staging_sqlserver)
                    logging.info("success updated data %s rows", modified.shape[0])

                else:
                    #jika ada data yang update, lakukan proses update yg lama, baru masukan data yg modified dan inserted
                    if len(modified['PatientID']) == 1:
                        patientid_update = modified['PatientID'].values[0]
                        query_update = f" UPDATE staging_rscm.DimensionPatientMPI SET ScdEnd = '{date}', ScdActive = 0 WHERE PatientID in ({patientid_update}) and ScdActive =1"
                        conn_staging_sqlserver.execute(query_update)
                        print('success update data existing')
                        logging.info("success update data existing")

                    else :
                        patientid_update = tuple(modified['PatientID'])
                        query_update = f"UPDATE staging_rscm.DimensionPatientMPI SET ScdEnd = '{date}', ScdActive = 0 WHERE PatientID in {patientid_update} and ScdActive =1"
                        conn_staging_sqlserver.execute(query_update)

                    create_surrogate_key(modified,conn_staging_sqlserver)
                    create_surrogate_key(inserted,conn_staging_sqlserver)  
                    print('success insert new data and updated data')
                    logging.info("success insert new data %s rows and updated data %s rows", inserted.shape[0], modified.shape[0])

        except Exception as e:
            print(e)

def send_email(subject, body):
    """Function untuk kirim notif ke email jika ada error"""
    try:
        logging.info("Preparing to send email notification.")
        
        # Email configuration
        sender_email = "lawkiddd2806@gmail.com"  # Replace with your email
        receiver_email = "lawkiddd2806@gmail.com"  # Replace with receiver's email
        password = "xoknfjvawuxitjvz"  # Replace with your app-specific password

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
    logging.info('Script Started')
    """ Info code process"""

    # Measure memory usage before code execution
    memory_before = process.memory_info().rss / (1024 * 1024)  # In MB
    """ Hitung Memori Awal sebelum code running"""
    logging.info("Memory before execution: %s", memory_before)

    t0 = time.time()
    """ Waktu awal sebelum code running"""

    try:
        # Konek DB MPI
        conn_mpi = connect_mpi()
        
        # Ambil data dari source MPI
        source = get_source_mpi(conn_mpi)
        print(source)
        logging.info("Extracted %s rows from source MPI", source.shape[0])

        # Konek DB EHR (untuk mengambil employee_id jika pasien tsb karyawan rscm dan detail pekerjaan pasien)
        conn_ehr = connect_ehr()
        employee_admission, occupation_patient = get_source_ehr(conn_ehr)
    
        # gabungkan dengan dataframe source dari MPI
        source = combine_mpi_ehr(source,employee_admission,occupation_patient)
        
        # Konek DB staging sqlserver
        conn_staging_sqlserver =  connect_staging()
        target = get_target_data(source, conn_staging_sqlserver)

        # Deteksi perubahan (buat dapetin data yang berubah dan baru)
        modified, inserted = detect_changes(source, target)
        logging.info("Total rows data modified: %s", modified.shape[0])
        logging.info("Here's sample data modified:\n%s", modified[['PatientID']].head(2).to_string(index=False))
        logging.info("Total rows data inserted: %s", inserted.shape[0])
        logging.info("Here's sample data inserted:\n%s", inserted[['PatientID']].head(2).to_string(index=False))

        
        # jalankan proses update dan insert data dengan menerapkan surrogate_key
        update_and_insert_data(target,modified,inserted,conn_staging_sqlserver)

    except Exception as e:
        # Log The Error
        error_message = (
            f"Dear Hanif,\n\n"
            f"There is an error in your script to DimensionPatientMPI, here's the error :\n\n"
            f"{str(e)}\n\n"
            f"Please investigate the issue.\n\n"
            f"Best regards,\nYour Monitoring Script"
        )
        logging.error(e)
        logging.error=error_message
        
        # Send email notification
        send_email(subject="Alert Error For DimensionPatientMPI", body=error_message)

    finally:
        db_connection.close_connection(conn_mpi)
        db_connection.close_connection(conn_ehr)
        db_connection.close_connection(conn_staging_sqlserver)

        # Measure memory usage after code execution
        memory_after = process.memory_info().rss / (1024 * 1024)  # In MB
        """ Memory setelah di code selesai running"""
        logging.info("Memory after execution: %s", memory_after)

        # Calculate memory used
        memory_used = memory_after - memory_before
        """ Total Memori yang terpakai untuk menjalankan program ini"""
        logging.info("Memory used: %s", memory_used)

        # hitung kecepatan eksekusi program
        t1 = time.time()
        total=t1-t0
        logging.info("total time execution: %s\n", total)

# Run the main process
if __name__ == "__main__":
    main()

