import datetime as dt
date = dt.datetime.today()
import time
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine as ce
import pandas as pd
import pyodbc

t0 = time.time()
mpi = ce("mysql://hanif-ppi:hanif2022@172.16.6.10/mpi")
sql_server = ce('mssql+pyodbc://andhi-ppi:Andhi2022!@172.16.19.36/StagingRSCM?driver=SQL+Server+Native+Client+11.0', fast_executemany=True) 
try :
    conn_mpi = mpi.connect()
    conn_mssql = sql_server.connect()
    print("successfully connect database")
except SQLAlchemyError as e:
    error = str(e.__dict__['orig'])
    print(error)

#ambil source
source = pd.read_sql_query(""" SELECT 
                                pt.patient_id as PatientID,
                                pt.mrn as MedicalNo, 
                                pr.person_nm as PatientName , 
                                ConvertMyDate(pr.date_of_birth) as BirthDate,
                                ifnull(pr.gender_cd,'') as Gender, 
                                ifnull(pr.marital_cd,'') as MaritalStatus ,  
                                pr.religion_id as  ReligionID,
                                ConvertMyDate(pt.created_dttm) as CreatedDate,
                                case when pr.regional_cd ='' then '-'  else pr.regional_cd end as RegionalCode,
                                pr.status_cd as StatusCode,
                                ConvertMyDate(pr.updated_dttm) as UpdatedDate,
                                pr.external_id as NIK, 
                                pr.address_txt  as Address
                                From  patients pt left join persons pr on pt.person_id = pr.person_id 
                                where  length(person_nm) > 1  
                                -- and pt.mrn ='10-04' data ditarik mulai 2016 saja
                                -- and DATE(pt.created_dttm) >= DATE(date_sub(now(), interval 1 DAY)) AND DATE(pt.created_dttm) < DATE(now())
                                and (DATE(pt.created_dttm) >= DATE(date_sub(now(), interval 1 DAY)) AND DATE(pt.created_dttm) < DATE(now()) OR DATE(pr.updated_dttm) >= DATE(date_sub(now(), interval 2 DAY)) AND DATE(pr.updated_dttm) < DATE(now()))
                                -- and (DATE(pt.created_dttm) = '2022-11-01' OR DATE(pr.updated_dttm) = '2022-11-01')
                                ORDER BY pt.patient_id """, conn_mpi)

#ambil primary key PatientID
patientid = tuple(source['PatientID'])

#tarik data dari target
query_full = f'SELECT PatientID,MedicalNo, PatientName,BirthDate,Gender, MaritalStatus, ReligionID,CreatedDate,RegionalCode,StatusCode,UpdatedDate,NIK,Address FROM staging_rscm.DimensionPatientMPI_Test order by '
target = pd.read_sql_query(query_full, conn_mssql)

#tarik data dari target tapi yang IsActive = 1
query_filter = f'SELECT PatientID,MedicalNo, PatientName,BirthDate,Gender, MaritalStatus, ReligionID,CreatedDate,RegionalCode,StatusCode,UpdatedDate,NIK,Address FROM staging_rscm.DimensionPatientMPI_Test WHERE PatientID IN {patientid} and SCDActive=1 order by PatientID '
target_filter = pd.read_sql_query(query_filter, conn_mssql)

# ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
change = source[~source.apply(tuple,1).isin(target_filter.apply(tuple,1))]

# ambil data dari variable change ambil primary key PatientID cek dengan variable target_filter, masukkan ke variable modified
modified = change[change[['PatientID']].apply(tuple,1).isin(target_filter[['PatientID']].apply(tuple,1))]

#tambahkan kolom SCDStart isi dengan tanggal, dan kolom IsActive beri nilai 1
modified['SCDStart']=date
modified['SCDActive']=1

# ambil data dari variable change, ambil primary key PatientID dan variable target_filter, masukkan ke variable inserted, kebalikan dari update pake simbol ~
inserted =  change[~change[['PatientID']].apply(tuple,1).isin(target_filter[['PatientID']].apply(tuple,1))]
inserted['SCDStart']=date
inserted['SCDActive']=1
print(modified)
print(inserted)

def CreateSurrogateAndPushData(df):
    new_query = f'SELECT PatientSurrogateKey FROM staging_rscm.DimensionPatientMPI_Test '
    new_target = pd.read_sql_query(new_query, conn_mssql)
    df.insert(0, 'PatientSurrogateKey', range(len(new_target)+1, len(new_target)+1 + len(df)))
    df.to_sql('DimensionPatientMPI_Test',schema='staging_rscm',con=conn_mssql,if_exists='append',index=False)

#jika target_filter kosong, maka semua data dari source langsung masuk ke table tujuan
if target_filter.empty:
    CreateSurrogateAndPushData(inserted)
    print('all data success inserted')
else:
    try:
        #jika tidak ada data yang update, data yg new record langsung masuk ke table tujuan
        if modified.empty:
            if inserted.empty:
                print('there is no new and updated data')
            else:
                CreateSurrogateAndPushData(inserted)
                print('success add new record without any update')
        else:
            #jika ada data yang update, lakukan proses update yg lama, baru masukan data yg modified dan inserted
            if len(modified['PatientID']) == 1:
                patientid_update = modified['PatientID'].values[0]
                print(patientid_update)
                query_update = f' UPDATE staging_rscm.DimensionPatientMPI_Test SET SCDEnd = staging_rscm.ConvertNow(), SCDActive = 0 WHERE PatientID in ({patientid_update}) and SCDActive =1'
                conn_mssql.execute(query_update)
            else :
                patientid_update = tuple(modified['PatientID'])
                query_update = f' UPDATE staging_rscm.DimensionPatientMPI_Test SET SCDEnd = staging_rscm.ConvertNow(), SCDActive = 0 WHERE PatientID in {patientid_update} and SCDActive =1'
                conn_mssql.execute(query_update) 
                    
            CreateSurrogateAndPushData(modified)
            print(modified)    
        
            CreateSurrogateAndPushData(inserted)
            print(inserted)
            print('success update data and add new record')

    except Exception as e:
        print(e)

#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)

conn_mssql.close()
conn_mpi.close()

