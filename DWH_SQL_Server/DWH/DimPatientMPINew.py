import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import datetime as dt
date = dt.datetime.today()
import time
import pandas as pd
import pyodbc

# bikin log ke file
sys.stdout = open("C:/TestPython/DWH_SQL_Server/DWH/logs/LogDimPatientMPINew.txt","w")
t0 = time.time()

#bikin koneksi ke db
conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)
conn_dwh_sqlserver = db_connection.create_connection(db_connection.dwh_sqlserver)

#ambil source
source = pd.read_sql_query(""" SELECT 
                                a.PatientSurrogateKey,
                                a.PatientID,
                                a.PersonID,
                                a.Confidentiality,
                                a.PatientStatus,
                                a.MigrationID,
                                a.MedicalNo,
                                UPPER(a.PatientName) PatientName,
                                a.BirthDate,
                                CASE
                                    WHEN a.PlaceOfBirth LIKE '%[0-9]%' OR a.PlaceOfBirth LIKE '%TDK%' THEN '-'
                                    ELSE UPPER(a.PlaceOfBirth)
                                END as PlaceOfBirth,
                                b.ReligionName,
                                CASE 
                                    WHEN a.BloodType = '' THEN '-'
                                    ELSE a.BloodType 
                                END AS BloodType,
                                a.Gender,
                                a.Occupation,
                                a.Education,
                                a.MaritalStatus,
                                CASE
                                    WHEN a.Nationality LIKE '%[0-9]%' 
                                    OR a.Nationality LIKE '%TDK%' 
                                    OR a.Nationality NOT LIKE '%[A-Z]%'
                                    OR a.Nationality LIKE '%TIDAK%'
                                    OR a.Nationality = '' THEN '-'
                                    WHEN a.Nationality LIKE '%WNI%'
                                    OR a.Nationality LIKE '%Indo%'
                                    OR a.Nationality LIKE '%esia%'
                                    THEN 'INDONESIA'
                                    ELSE UPPER(a.Nationality)
                                END AS Nationality,
                                a.PatientCreatedDate,
                                f.DeceasedDate as PatientDeceasedDate,
                                a.PatientUpdatedDate,
                                CASE
                                    WHEN a.NIK LIKE '%[A-Z]%'
                                    OR a.NIK LIKE '%TDK%' 
                                    OR a.NIK = '' THEN '-'
                                    ELSE a.NIK
                                END AS NIK,
                                CASE
                                    WHEN a.TelephoneNo LIKE '%[A-Z]%'
                                    OR a.TelephoneNo LIKE '%TDK%' 
                                    OR a.TelephoneNo = '' THEN '-'
                                    ELSE a.TelephoneNo
                                END AS TelephoneNo,
                                CASE
                                    WHEN a.PhoneNo LIKE '%[A-Z]%'
                                    OR a.PhoneNo LIKE '%TDK%' 
                                    OR a.PhoneNo = '' THEN '-'
                                    ELSE a.PhoneNo
                                END AS PhoneNo,
                                CASE
                                    WHEN d.BPJSNo = '' THEN '-'
                                    ELSE d.BPJSNo
                                END AS BPJSNo,
                                CASE
                                    WHEN a.Email = '' OR a.Email NOT LIKE '%@%' 
                                    OR a.Email LIKE '%TDK%'
                                    OR a.Email LIKE '%Tidak%' THEN '-'
                                    ELSE a.Email
                                END as Email,
                                a.Address,
                                c.District,
                                c.County,
                                c.City,
                                c.Province,
                                CASE
                                    WHEN a.PostalCode = '' OR 
                                    a.PostalCode LIKE '%[A-Z]%' THEN '-'
                                    ELSE a.PostalCode
                                END as PostalCode, 
                                a.PatientNullifiedDate,
                                a.PersonStatusCode,
                                a.PatientStatusCode,
                                a.PersonTitle,
                                a.FamilyName,
                                a.ExternalID,
                                CASE
                                    WHEN a.PassportNo LIKE '%[A-Z]%' OR a.PassportNo = '' THEN '-' 
                                    ELSE a.PassportNo
                                END AS PassportNo,
                                CASE
                                    WHEN a.FatherName LIKE '%[0-9]%' OR a.MotherName LIKE '%TDK%' OR a.FatherName NOT LIKE '%[A-Z]%' THEN '-' 
                                    ELSE UPPER(a.FatherName) 
                                END AS FatherName,
                                CASE
                                    WHEN a.MotherName LIKE '%[0-9]%' OR a.MotherName LIKE '%TDK%' OR a.MotherName NOT LIKE '%[A-Z]%' THEN '-' 
                                    ELSE UPPER(a.MotherName) 
                                END AS MotherName,
                                CASE
                                    WHEN a.SpouseName LIKE '%[0-9]%' OR a.MotherName LIKE '%TDK%' OR a.SpouseName NOT LIKE '%[A-Z]%' THEN '-' 
                                    ELSE UPPER(a.SpouseName) 
                                END AS SpouseName,
                                a.Race,
                                a.ScdActive,
                                a.ScdStart,
                                a.ScdEnd,
                                a.EmployeeID,
                                a.IsEmployee,
                                a.InsertedDateDWH
                            FROM staging_rscm.DimensionPatientMPI a
                            LEFT JOIN DWH_RSCM.dwhrscm_talend.DimReligion b on a.ReligionID = b.ReligionID
                            LEFT JOIN DWH_RSCM.dwhrscm_talend.DimRegional c on a.RegionalCode = c.RegionalCode
                            LEFT JOIN (SELECT x.PatientID, x.BPJSNo 
                                        FROM
                                        (SELECT PatientID,
                                            BPJSNo, 
                                            DENSE_RANK()OVER(PARTITION BY PatientID ORDER BY AdmissionID DESC) rank
                                        FROM  staging_rscm.TransPatientAdmission
                                        WHERE BPJSNo IS NOT NULL OR BPJSNo = ''
                                        ) x
                                        WHERE x.rank = 1
                                        ) d on a.PatientID = d.PatientID
                            LEFT JOIN DWH_RSCM.dwhrscm_talend.FactPatientDeceased f on a.PatientID = f.PatientID and f.SCDActive = 1    
                            WHERE
                            -- PatientID IN (1657653) 
                            -- PatientCreatedDate >= '2023-03-11 00:00:00' and PatientCreatedDate <= '2023-03-15 23:59:59'  
                            -- (CAST(a.InsertedDateDWH as date) >= '2024-04-10' AND CAST(a.InsertedDateDWH as date) <= '2024-04-16')
                            -- OR (CAST(a.ScdEnd as date) >= '2024-04-10' AND CAST(a.ScdEnd as date) <= '2024-04-16')
                            (CAST(a.InsertedDateDWH as date) >= CAST(DATEADD(DAY, -1, GETDATE()) as date) AND CAST(a.InsertedDateDWH as date) <= CAST(GETDATE() as date))
                            OR (CAST(a.ScdEnd as date) >= CAST(DATEADD(DAY, -1, GETDATE()) as date) AND CAST(a.ScdEnd as date) <= CAST(GETDATE() as date))
                            -- WHERE CAST(ScdStart as date) = CAST(GETDATE() as date)
 """, conn_staging_sqlserver)
print(source)

source['BirthDate'] = pd.to_datetime(source['BirthDate'], format="%Y-%m-%d %H:%M:%S", errors='coerce').dt.strftime("%Y-%m-%d %H:%M:%S")
source['PatientCreatedDate'] = pd.to_datetime(source['PatientCreatedDate'], format="%Y-%m-%d %H:%M:%S", errors='coerce').dt.strftime("%Y-%m-%d %H:%M:%S")
source['PatientUpdatedDate'] = pd.to_datetime(source['PatientUpdatedDate'], format="%Y-%m-%d %H:%M:%S")
source['PatientNullifiedDate'] = pd.to_datetime(source['PatientNullifiedDate'], format="%Y-%m-%d %H:%M:%S")
source['ScdStart'] = pd.to_datetime(source['ScdStart'], format="%Y-%m-%d %H:%M:%S")
source['ScdEnd'] = pd.to_datetime(source['ScdEnd'], format="%Y-%m-%d %H:%M:%S")
source['InsertedDateDWH'] = pd.to_datetime(source['InsertedDateDWH'], format="%Y-%m-%d %H:%M:%S")

if source.empty:
    print('tidak ada data dari source')
else :
    patientsurrogatekey = tuple(source["PatientSurrogateKey"].unique())
    if len(patientsurrogatekey) > 1:
        pass
    else:
        patientsurrogatekey = str(patientsurrogatekey).replace(',','')

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
                    PlaceOfBirth,
                    ReligionName,
                    BloodType,
                    TRIM(Gender) AS Gender,
                    Occupation,
                    Education,
                    MaritalStatus,
                    Nationality,
                    PatientCreatedDate,
                    PatientDeceasedDate,
                    PatientUpdatedDate,
                    NIK,
                    TelephoneNo,
                    PhoneNo,
                    BPJSNo,
                    Email
                    Address,
                    District,
                    County,
                    City,
                    Province,
                    PostalCode,
                    PatientNullifiedDate,
                    PersonStatusCode,
                    PatientStatusCode,
                    PersonTitle,
                    FamilyName,
                    ExternalID,
                    PassportNo,
                    FatherName,
                    MotherName,
                    SpouseName,
                    Race,
                    ScdActive,
                    ScdStart,
                    ScdEnd,
                    EmployeeID,
                    IsEmployee,
                    InsertedDateDWH
                FROM dwhrscm_talend.DimPatientMPINew
                WHERE PatientSurrogateKey IN {patientsurrogatekey}
                ORDER BY PatientSurrogateKey """
    target = pd.read_sql_query(query, conn_dwh_sqlserver)

print(source.dtypes)
print(target.dtypes)

# ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
change = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

# ambil data dari variable change ambil primary key PatientID cek dengan variable target, masukkan ke variable modified
modified = change[change[['PatientSurrogateKey']].apply(tuple,1).isin(target[['PatientSurrogateKey']].apply(tuple,1))]

# ambil data dari variable change, ambil primary key PatientID dan variable target, masukkan ke variable inserted, kebalikan dari update pake simbol ~
inserted =  change[~change[['PatientSurrogateKey']].apply(tuple,1).isin(target[['PatientSurrogateKey']].apply(tuple,1))]

print('ini modified')
print(modified)

print('ini insert')
print(inserted)

if modified.empty:
    inserted.to_sql('DimPatientMPINew', schema='dwhrscm_talend', con = conn_dwh_sqlserver, if_exists = 'append', index=False)
    print('success insert all data without update')
    
else:
    # buat fungsi untuk update data ke tabel target
    def updated_to_sql(df, table_name, key_1):
        list_col = []
        table=table_name
        pk_1 = key_1
        temp_table = f'{table}_temporary_table'
        for col in df.columns:
            if col == pk_1 :
                continue
            list_col.append(f'r.{col} = t.{col}')
        df.to_sql(temp_table,schema = 'dwhrscm_talend',con=conn_dwh_sqlserver, if_exists='replace',index=False)
        update_stmt_1 = f'UPDATE r '
        update_stmt_2 = f'SET '
        update_stmt_3 = ", ".join(list_col)
        update_stmt_4 = f' FROM dwhrscm_talend.{table} r '
        update_stmt_5 = f'INNER JOIN (SELECT * FROM dwhrscm_talend.{temp_table}) as t ON r.{pk_1} = t.{pk_1} '
        update_stmt_6 = f'WHERE r.{pk_1} = t.{pk_1} '
        update_stmt_7 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_4 + update_stmt_5 + update_stmt_6 +";"
        delete_stmt_1 = f'DROP TABLE dwhrscm_talend.{temp_table}'
        print(update_stmt_7)
        conn_dwh_sqlserver.execute(update_stmt_7)
        conn_dwh_sqlserver.execute(delete_stmt_1)

    try:
        # update data
        updated_to_sql(modified, 'DimPatientMPINew', 'PatientSurrogateKey')

        # insert data baru
        inserted.to_sql('DimPatientMPINew', schema='dwhrscm_talend', con=conn_dwh_sqlserver, if_exists ='append',index=False)
        print('success update and insert all data')
    
    except Exception as e:
        print(e)

#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)

text= f'scheduler tanggal : {date}'
print(text)
sys.stdout.close()

db_connection.close_connection(conn_dwh_sqlserver)
db_connection.close_connection(conn_staging_sqlserver)
