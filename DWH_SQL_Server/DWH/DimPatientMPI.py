import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import datetime as dt
date = dt.datetime.today()
import time
import pandas as pd
import pyodbc

# bikin log ke file
sys.stdout = open("C:/TestPython/DWH_SQL_Server/DWH/logs/LogDimPatientMPI.txt","w")
t0 = time.time()

#bikin koneksi ke db
conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)
conn_dwh_sqlserver = db_connection.create_connection(db_connection.dwh_sqlserver)

#ambil source
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
    if len(source) == 1:        
        # ambil primary key dari source, ambil index ke 0
        patientsurrogatekey = source["PatientSurrogateKey"].values[0]

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
                    WHERE PatientSurrogateKey IN ({patientsurrogatekey})
                    ORDER BY PatientSurrogateKey """
        target = pd.read_sql_query(query, conn_dwh_sqlserver)
    else :
         # ambil primary key dari source, pake unique biar tidak duplicate
        patientsurrogatekey = tuple(source["PatientSurrogateKey"].unique())

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
    inserted.to_sql('DimPatientMPI', schema='dwhrscm_talend', con = conn_dwh_sqlserver, if_exists = 'append', index=False)
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
        updated_to_sql(modified, 'DimPatientMPI', 'PatientSurrogateKey')

        # insert data baru
        inserted.to_sql('DimPatientMPI', schema='dwhrscm_talend', con=conn_dwh_sqlserver, if_exists ='append',index=False)
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
