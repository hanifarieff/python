import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import datetime as dt
date = dt.datetime.today()
import time
from datetime import timedelta
import pandas as pd
import pyodbc
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# bikin log ke file
sys.stdout = open("C:/TestPython/DWH_SQL_Server/Staging/logs/LogDimensionPatientMPINew.txt","w")
t0 = time.time()

# log path file error
log_file_path = "C:/TestPython/DWH_SQL_Server/Staging/Log/DimensionPatientMPI.log"


# Setup logging to capture all levels
logging.basicConfig(
    filename=log_file_path,
    level=logging.DEBUG,  # Set the lowest level to capture everything
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def send_email(subject, body):
    """Send an email notification."""
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
            logging.debug("Connecting to SMTP server.")
            server.starttls()
            server.login(sender_email, password)
            server.send_message(msg)
        logging.info("Email sent successfully.")
    except Exception as e:
        logging.error("Failed to send email notification: %s", str(e))

def main():
    try:
        # bikin koneksi ke db
       
        conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)
        conn_mpi = db_connection.create_connection(db_connection.mpi)

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
                                    -- pt.patient_id IN (2091184) AND
                                    (pt.created_dttm >= '2025-01-24 00:00:00' and pt.created_dttm <= '2025-01-24 05:59:59')
                                    -- (pr.updated_dttm >= '2023-10-12 00:00:00' and pr.updated_dttm <= '2023-10-12 23:59:59')
                                    -- (pt.created_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 00:00:00") 
                                    -- AND pt.created_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 23:59:59")) 
                                    -- AND pr.person_nm IS NOT NULL""", conn_mpi)

        # # # tarik data yang update berdasarkan kolom update di tabel person
        # source2 = pd.read_sql_query(""" SELECT 
        #                                 pt.patient_id as PatientID,
        #                                 pt.person_id as PersonID,
        #                                 TRIM(pt.confidentiality_cd) as Confidentiality,
        #                                 pt.status_cd as PatientStatus,
        #                                 pt.migration_id as MigrationID,
        #                                 pt.mrn as MedicalNo, 
        #                                 pr.person_nm as PatientName , 
        #                                 pr.date_of_birth as BirthDate,
        #                                 ifnull(pr.gender_cd,'') as Gender, 
        #                                 ifnull(pr.marital_cd,'') as MaritalStatus ,  
        #                                 pr.religion_id as  ReligionID,  
        #                                 pr.external_id as NIK, 
        #                                 pr.address_txt  as Address, 
        #                                 pt.created_dttm as PatientCreatedDate, 
        #                                 pt.updated_dttm as PatientUpdatedDate, 
        #                                 pt.nullified_dttm as PatientNullifiedDate, 
        #                                 case when pr.regional_cd ='' then '-'  else pr.regional_cd end as RegionalCode,
        #                                 pr.status_cd as PersonStatusCode,
        #                                 pt.status_cd as PatientStatusCode,
        #                                 -- pr.updated_dttm as UpdatedDate
        #                                 -- ConvertMyDate(pr.updated_dttm) as PersonUpdatedDate,
        #                                 person_title as PersonTitle,
        #                                 family_nm as FamilyName,
        #                                 external_id as ExternalID,
        #                                 passport_no as PassportNo,
        #                                 father_nm as FatherName,
        #                                 mother_nm as MotherName,
        #                                 spouse_nm as SpouseName,
        #                                 place_of_birth as PlaceOfBirth,
        #                                 TRIM(postal_cd) as PostalCode,
        #                                 pr.race as Race,
        #                                 edu.educlvl_nm as Education,
        #                                 pr.jobclass_id,
        #                                 pr.blood_type as BloodType,
        #                                 pr.nationality as Nationality,
        #                                 pr.phone1 as TelephoneNo,
        #                                 pr.phone3 as PhoneNo,
        #                                 pr.email as Email
        #                             FROM  patients pt left join persons pr on pt.person_id = pr.person_id 
        #                             LEFT JOIN hris_sys_cd_educlvl edu ON edu.educlvl_cd = pr.education_id
        #                             WHERE 
        #                             -- pt.patient_id IN (2061500,2061501)
        #                             -- pr.updated_dttm >= '2024-10-23 00:00:00' and pr.updated_dttm <= '2024-10-23 23:59:59'
        #                             (pr.updated_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 00:00:00") 
        #                             and pr.updated_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 23:59:59"))
        #                             -- AND pr.person_nm IS NOT NULL""", conn_mpi)

        # gabungkan source1 dan source2, lalu pisahkan data yang duplikat berdasarkan PatientID
        # source = pd.concat([source1, source2], ignore_index=True)
        source= source1
        # source = source.drop_duplicates(subset='PatientID')
        print(source)
        logging.info("This is output source:\n%s", source.head(20).to_string(index=False))

        # bikin koneksi ke db
        conn_ehr = db_connection.create_connection(db_connection.replika_ehr)

        # buat cek apakah patient tersebut pegawai rscm atau tidak
        employee_admission = pd.read_sql_query(""" SELECT patient_id as PatientID, employee_id EmployeeID FROM xocp_ehr_employee_admission""", conn_ehr)
        # print(source)

        # join ke tabel ini di ehr buat dapetin pekerjaan pasien
        occupation_patient =  pd.read_sql_query(""" SELECT jobclass_cd as jobclass_id, jobclass_nm AS Occupation FROM xocp_sys_cd_jobclass""",conn_ehr)

        #left join ke employee_admission buat ambil EmployeeID
        source = source.merge(employee_admission,how='left',on='PatientID').merge(occupation_patient,how='left',on='jobclass_id')

        new_order_columns = ['PatientID','PersonID','Confidentiality','PatientStatus','MigrationID','MedicalNo','PatientName','BirthDate',
                            'Gender','MaritalStatus','ReligionID','NIK','Address','PatientCreatedDate','PatientUpdatedDate',
                            'PatientNullifiedDate','RegionalCode','PersonStatusCode','PatientStatusCode','PersonTitle','FamilyName','ExternalID',
                            'PassportNo','FatherName','MotherName','SpouseName','PlaceOfBirth','PostalCode','Race','Education','Occupation',
                            'BloodType','Nationality','TelephoneNo','PhoneNo','Email','EmployeeID','IsEmployee']
        source = source.reindex(columns=new_order_columns)
        logging.info("This is source after join:\n%s", source.head(5).to_string(index=False))

        #bikin kondisi untuk kolom IsEmployee kalo EmployeeID null = 0, tidak null = 1
        source.loc[source['EmployeeID'].isnull(),'IsEmployee']=0
        source.loc[source['EmployeeID'].notnull(),'IsEmployee']=1

        #rubah tipe data EmployeeID dan IsEmployee jadi integer
        source['EmployeeID'] = source['EmployeeID'].fillna(0).astype('int64')
        source['IsEmployee'] = source['IsEmployee'].fillna(0).astype('int64')
        source['Occupation'].fillna('-',inplace=True)

        print(source)

        if source.empty:
            print('tidak ada data dari source')
        else :
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
            target_filter = pd.read_sql_query(query_filter,conn_staging_sqlserver)

        query_drop_table = f'DROP TABLE staging_rscm.DimensionPatientMPITemporary'
        conn_staging_sqlserver.execute(query_drop_table)

        # print(source.iloc[:,0:3].apply(tuple,1).isin(target_filter.iloc[:,0:3].apply(tuple,1)))
        # print(source.iloc[:,3:5].apply(tuple,1).isin(target_filter.iloc[:,3:5].apply(tuple,1)))
        # print(source.iloc[:,5:7].apply(tuple,1).isin(target_filter.iloc[:,5:7].apply(tuple,1)))
        # print(source.iloc[:,8:9].apply(tuple,1).isin(target_filter.iloc[:,8:9].apply(tuple,1)))

        # print(source.iloc[:,0:3].apply(tuple,1))
        # print(target_filter.iloc[:,0:3].apply(tuple,1))
        # print(source.iloc[:,2:4].apply(tuple,1))
        # print(target_filter.iloc[:,2:4].apply(tuple,1))
        # print(source.iloc[:,3:5].apply(tuple,1))
        # print(target_filter.iloc[:,3:5].apply(tuple,1))
        # print(source.iloc[:,4:6].apply(tuple,1))
        # print(target_filter.iloc[:,4:6].apply(tuple,1))
        # print(source.iloc[:,5:7].apply(tuple,1))
        # print(target_filter.iloc[:,5:7].apply(tuple,1))
        # print('bates')
        # print(source.iloc[:,7:9].apply(tuple,1))
        # print(target_filter.iloc[:,7:9].apply(tuple,1))
        # print(source.iloc[:,9:11].apply(tuple,1))
        # print(target_filter.iloc[:,9:11].apply(tuple,1))
        # print(source.iloc[:,11:13].apply(tuple,1))
        # print(target_filter.iloc[:,11:13].apply(tuple,1))
        # print(source.iloc[:,13:15].apply(tuple,1))
        # print(target_filter.iloc[:,13:15].apply(tuple,1))
        # print(source.iloc[:,15:17].apply(tuple,1))
        # print(target_filter.iloc[:,15:17].apply(tuple,1))
        # print(source.iloc[:,17:20].apply(tuple,1))
        # print(target_filter.iloc[:,17:20].apply(tuple,1))
        # print(source.iloc[:,20:24].apply(tuple,1))
        # print(target_filter.iloc[:,20:24].apply(tuple,1))
        # print('bates lagi')
        # print(source.iloc[:,21:25].apply(tuple,1))
        # print(target_filter.iloc[:,21:25].apply(tuple,1))
        # print(source.iloc[:,25:31].apply(tuple,1))
        # print(target_filter.iloc[:,25:31].apply(tuple,1))

        # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
        change = source[~source.apply(tuple,1).isin(target_filter.apply(tuple,1))]

        # ambil data dari variable change ambil primary key PatientID cek dengan variable target_filter, masukkan ke variable modified
        modified = change[change[['PatientID']].apply(tuple,1).isin(target_filter[['PatientID']].apply(tuple,1))]

        # tambahkan kolom ScdStart isi dengan tanggal, dan kolom ScdActive beri nilai 1
        modified['ScdStart']=date
        modified['ScdActive']=1

        # ambil data dari variable change, ambil primary key PatientID dan variable target_filter, masukkan ke variable inserted, kebalikan dari update pake simbol ~
        inserted =  change[~change[['PatientID']].apply(tuple,1).isin(target_filter[['PatientID']].apply(tuple,1))]
        inserted['ScdStart']=date
        inserted['ScdActive']=1

        print('ini modified')
        print(modified)
        logging.info("dataframe modified:\n%s", modified.head(10).to_string(index=False))

        print('ini insert')
        print(inserted)
        logging.info("dataframe inserted:\n%s", inserted.head(10).to_string(index=False))


        def CreateSurrogateAndPushData(df):
            new_query = f'SELECT PatientSurrogateKey FROM staging_rscm.DimensionPatientMPI '
            new_target = pd.read_sql_query(new_query, conn_staging_sqlserver)
            df.insert(0, 'PatientSurrogateKey', range(len(new_target)+1, len(new_target)+1 + len(df)))
            df.to_sql('DimensionPatientMPI',schema='staging_rscm',con=conn_staging_sqlserver,if_exists='append',index=False)


        #jika target_filter kosong, maka semua data dari source langsung masuk ke table tujuan
        if target_filter.empty:
            CreateSurrogateAndPushData(inserted)
            # inserted.insert(0, 'PatientSurrogateKey', range(len(target)+1, len(target)+1 + len(inserted)))
            # inserted.to_sql('tes_patient_target',schema='dbo',con=conn_mssql,if_exists='append',index=False)
            print('all data success inserted')
            logging.info("all data success inserted")

        else:
            try:
                #jika tidak ada data yang update, data yg new record langsung masuk ke table tujuan
                if modified.empty:
                    if inserted.empty:
                        print('there is no new and updated data')
                        logging.info("there is no new and updated data")
                    else:
                        CreateSurrogateAndPushData(inserted)
                        print('success add new record without any update')
                        logging.info("success add new record without any update")
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

                    CreateSurrogateAndPushData(modified)
                    CreateSurrogateAndPushData(inserted)  
                    print('success insert new data and updated data')
                    logging.info("success insert new data and updated data")

            except Exception as e:
                print(e)
    except Exception as e:
        # Log The Error
        error_message = (
            f"Dear Hanif,\n\n"
            f"There is an error in your script to DimPatientMPI, here's the error :\n\n"
            f"{str(e)}\n\n"
            f"Please investigate the issue.\n\n"
            f"Best regards,\nYour Monitoring Script"
        )
        logging.error=error_message
        
        # Send email notification
        send_email(subject="Alert Error For DimensionPatientMPI", body=error_message)

    finally:
        logging.info("Script finished.")
        db_connection.close_connection(conn_mpi)
        db_connection.close_connection(conn_ehr)
        db_connection.close_connection(conn_staging_sqlserver)

if __name__ == "__main__":
    main()

