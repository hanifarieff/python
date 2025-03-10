import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import datetime as dt
date = dt.datetime.today()
import time
import pandas as pd
import pyodbc

# bikin log ke file
sys.stdout = open("C:/TestPython/DWH_SQL_Server/Staging/logs/LogDimensionPatientMPITempo.txt","w")
t0 = time.time()

# bikin koneksi ke db
conn_ehr = db_connection.create_connection(db_connection.replika_ehr)
conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)
conn_dwh_sqlserver = db_connection.create_connection(db_connection.dwh_sqlserver)
conn_mpi = db_connection.create_connection(db_connection.mpi)


source_mpi = pd.read_sql_query(""" select PatientID from dwhrscm_talend.PatientMPITemporary
where AdmissionDate >= '2023-01-01' and AdmissionDate <= '2023-01-31'
    """, conn_dwh_sqlserver)

patientid = tuple(source_mpi['PatientID'])

# tarik data berdasarkan tanggal create tabel patients
query_source = f""" SELECT 
                                pt.patient_id as PatientID,
                                pt.person_id as PersonID,
                                TRIM(pt.confidentiality_cd) as Confidentiality,
                                pt.status_cd as PatientStatus,
                                pt.migration_id as MigrationID,
                                pt.mrn as MedicalNo, 
                                pr.person_nm as PatientName , 
                                ConvertMyDate(pr.date_of_birth) as BirthDate,
                                ifnull(pr.gender_cd,'') as Gender, 
                                ifnull(pr.marital_cd,'') as MaritalStatus ,  
                                pr.religion_id as  ReligionID,  
                                pr.external_id as NIK, 
                                pr.address_txt  as Address, 
                                ConvertMyDate(pt.created_dttm) as PatientCreatedDate, 
                                ConvertMyDate(pt.updated_dttm) as PatientUpdatedDate, 
                                ConvertMyDate(pt.nullified_dttm) as PatientNullifiedDate, 
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
                                pr.jobclass_id
                            FROM  patients pt left join persons pr on pt.person_id = pr.person_id 
                            LEFT JOIN hris_sys_cd_educlvl edu ON edu.educlvl_cd = pr.education_id
                            WHERE 
                            -- pt.patient_id IN (1853237)
                            -- (pt.created_dttm >= '2024-02-10 00:00:00' and pt.created_dttm <= '2024-02-10 23:59:59')
                            -- (pr.updated_dttm >= '2023-10-12 00:00:00' and pr.updated_dttm <= '2023-10-12 23:59:59')
                            pt.patient_id IN  {patientid}
                            -- AND pr.person_nm IS NOT NULL
 """
source = pd.read_sql_query(query_source, conn_mpi)

print(source)

employee_admission = pd.read_sql_query(""" SELECT patient_id as PatientID, employee_id EmployeeID FROM xocp_ehr_employee_admission""", conn_ehr)
# print(source)

# join ke tabel ini di ehr buat dapetin pekerjaan pasien
occupation_patient =  pd.read_sql_query(""" SELECT jobclass_cd as jobclass_id, jobclass_nm AS Occupation FROM xocp_sys_cd_jobclass""",conn_ehr)

#left join ke employee_admission buat ambil EmployeeID
source = source.merge(employee_admission,how='left',on='PatientID').merge(occupation_patient,how='left',on='jobclass_id')

new_order_columns = ['PatientID','PersonID','Confidentiality','PatientStatus','MigrationID','MedicalNo','PatientName','BirthDate',
                    'Gender','MaritalStatus','ReligionID','NIK','Address','PatientCreatedDate','PatientUpdatedDate',
                    'PatientNullifiedDate','RegionalCode','PersonStatusCode','PatientStatusCode','PersonTitle','FamilyName','ExternalID',
                    'PassportNo','FatherName','MotherName','SpouseName','PlaceOfBirth','PostalCode','Race','Education','Occupation','EmployeeID','IsEmployee']
source = source.reindex(columns=new_order_columns)

#bikin kondisi untuk kolom IsEmployee kalo EmployeeID null = 0, tidak null = 1
source.loc[source['EmployeeID'].isnull(),'IsEmployee']=0
source.loc[source['EmployeeID'].notnull(),'IsEmployee']=1

#rubah tipe data EmployeeID dan IsEmployee jadi integer
source['EmployeeID'] = source['EmployeeID'].fillna(0).astype('int64')
source['IsEmployee'] = source['IsEmployee'].fillna(0).astype('int64')
source['Occupation'].fillna('-',inplace=True)

print(source)

# if source.empty:
#     print('tidak ada data dari source')
# else :
#     source.to_sql('DimensionPatientMPITemporary', schema='staging_rscm',con = conn_staging_sqlserver, if_exists = 'append', index=False)
#     query_filter = f""" 
#                     SELECT 
#                         PatientID,
#                         PersonID,
#                         TRIM(Confidentiality) AS Confidentiality,
#                         PatientStatus,
#                         MigrationID,
#                         MedicalNo,
#                         PatientName,
#                         BirthDate,
#                         TRIM(Gender) AS Gender,
#                         MaritalStatus,
#                         ReligionID,
#                         NIK,
#                         Address,
#                         PatientCreatedDate,
#                         PatientUpdatedDate,
#                         PatientNullifiedDate,
#                         RegionalCode,
#                         PersonStatusCode,
#                         PatientStatusCode,
#                         PersonTitle,
#                         FamilyName,
#                         ExternalID,
#                         PassportNo,
#                         FatherName,
#                         MotherName,
#                         SpouseName,
#                         PlaceOfBirth,
#                         TRIM(PostalCode) AS PostalCode,
#                         Race,
#                         Education,
#                         Occupation,
#                         EmployeeID,
#                         IsEmployee
#                     FROM staging_rscm.DimensionPatientMPI 
#                     WHERE PatientID IN (SELECT PatientID FROM staging_rscm.DimensionPatientMPITemporary) 
#                     AND ScdActive = 1
#                     ORDER BY PatientID """
#     target_filter = pd.read_sql_query(query_filter,conn_staging_sqlserver)

# query_drop_table = f'DROP TABLE staging_rscm.DimensionPatientMPITemporary'
# conn_staging_sqlserver.execute(query_drop_table)

# # print(source.iloc[:,0:3].apply(tuple,1).isin(target_filter.iloc[:,0:3].apply(tuple,1)))
# # print(source.iloc[:,3:5].apply(tuple,1).isin(target_filter.iloc[:,3:5].apply(tuple,1)))
# # print(source.iloc[:,5:7].apply(tuple,1).isin(target_filter.iloc[:,5:7].apply(tuple,1)))
# # print(source.iloc[:,8:9].apply(tuple,1).isin(target_filter.iloc[:,8:9].apply(tuple,1)))

# # print(source.iloc[:,0:3].apply(tuple,1))
# # print(target_filter.iloc[:,0:3].apply(tuple,1))
# # print(source.iloc[:,2:4].apply(tuple,1))
# # print(target_filter.iloc[:,2:4].apply(tuple,1))
# # print(source.iloc[:,3:5].apply(tuple,1))
# # print(target_filter.iloc[:,3:5].apply(tuple,1))
# # print(source.iloc[:,4:6].apply(tuple,1))
# # print(target_filter.iloc[:,4:6].apply(tuple,1))
# # print(source.iloc[:,5:7].apply(tuple,1))
# # print(target_filter.iloc[:,5:7].apply(tuple,1))
# # print('bates')
# # print(source.iloc[:,7:9].apply(tuple,1))
# # print(target_filter.iloc[:,7:9].apply(tuple,1))
# # print(source.iloc[:,9:11].apply(tuple,1))
# # print(target_filter.iloc[:,9:11].apply(tuple,1))
# # print(source.iloc[:,11:13].apply(tuple,1))
# # print(target_filter.iloc[:,11:13].apply(tuple,1))
# # print(source.iloc[:,13:15].apply(tuple,1))
# # print(target_filter.iloc[:,13:15].apply(tuple,1))
# # print(source.iloc[:,15:17].apply(tuple,1))
# # print(target_filter.iloc[:,15:17].apply(tuple,1))
# # print(source.iloc[:,17:20].apply(tuple,1))
# # print(target_filter.iloc[:,17:20].apply(tuple,1))
# # print(source.iloc[:,20:24].apply(tuple,1))
# # print(target_filter.iloc[:,20:24].apply(tuple,1))
# # print('bates lagi')
# # print(source.iloc[:,21:25].apply(tuple,1))
# # print(target_filter.iloc[:,21:25].apply(tuple,1))
# # print(source.iloc[:,25:31].apply(tuple,1))
# # print(target_filter.iloc[:,25:31].apply(tuple,1))

# # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
# change = source[~source.apply(tuple,1).isin(target_filter.apply(tuple,1))]

# # ambil data dari variable change ambil primary key PatientID cek dengan variable target_filter, masukkan ke variable modified
# modified = change[change[['PatientID']].apply(tuple,1).isin(target_filter[['PatientID']].apply(tuple,1))]

# # tambahkan kolom ScdStart isi dengan tanggal, dan kolom ScdActive beri nilai 1
# modified['ScdStart']=date
# modified['ScdActive']=1

# # ambil data dari variable change, ambil primary key PatientID dan variable target_filter, masukkan ke variable inserted, kebalikan dari update pake simbol ~
# inserted =  change[~change[['PatientID']].apply(tuple,1).isin(target_filter[['PatientID']].apply(tuple,1))]
# inserted['ScdStart']=date
# inserted['ScdActive']=1

# print('ini modified')
# print(modified)

# print('ini insert')
# print(inserted)

# def CreateSurrogateAndPushData(df):
#     new_query = f'SELECT PatientSurrogateKey FROM staging_rscm.DimensionPatientMPI '
#     new_target = pd.read_sql_query(new_query, conn_staging_sqlserver)
#     df.insert(0, 'PatientSurrogateKey', range(len(new_target)+1, len(new_target)+1 + len(df)))
#     df.to_sql('DimensionPatientMPI',schema='staging_rscm',con=conn_staging_sqlserver,if_exists='append',index=False)


# #jika target_filter kosong, maka semua data dari source langsung masuk ke table tujuan
# if target_filter.empty:
#     CreateSurrogateAndPushData(inserted)
#     # inserted.insert(0, 'PatientSurrogateKey', range(len(target)+1, len(target)+1 + len(inserted)))
#     # inserted.to_sql('tes_patient_target',schema='dbo',con=conn_mssql,if_exists='append',index=False)
#     print('all data success inserted')
# else:
#     try:
#         #jika tidak ada data yang update, data yg new record langsung masuk ke table tujuan
#         if modified.empty:
#             if inserted.empty:
#                 print('there is no new and updated data')
#             else:
#                 CreateSurrogateAndPushData(inserted)
#                 print('success add new record without any update')
#         else:
#             #jika ada data yang update, lakukan proses update yg lama, baru masukan data yg modified dan inserted
#             if len(modified['PatientID']) == 1:
#                 patientid_update = modified['PatientID'].values[0]
#                 query_update = f" UPDATE staging_rscm.DimensionPatientMPI SET ScdEnd = '{date}', ScdActive = 0 WHERE PatientID in ({patientid_update}) and ScdActive =1"
#                 conn_staging_sqlserver.execute(query_update)
#                 print('success update data existing')

#             else :
#                 patientid_update = tuple(modified['PatientID'])
#                 query_update = f"UPDATE staging_rscm.DimensionPatientMPI SET ScdEnd = '{date}', ScdActive = 0 WHERE PatientID in {patientid_update} and ScdActive =1"
#                 conn_staging_sqlserver.execute(query_update)

#             CreateSurrogateAndPushData(modified)
#             CreateSurrogateAndPushData(inserted)  
#             print('success insert new data and updated data')

#     except Exception as e:
#         print(e)

#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)

text= f'scheduler tanggal : {date}'
print(text)
sys.stdout.close()

db_connection.close_connection(conn_mpi)
db_connection.close_connection(conn_ehr)
db_connection.close_connection(conn_staging_sqlserver)