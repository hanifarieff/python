import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import datetime as dt
date = dt.datetime.today()
import time
import pandas as pd
import pyodbc

# bikin log ke file
sys.stdout = open("C:/TestPython/DWH_SQL_Server/DWH/logs/LogDimBed.txt","w")
t0 = time.time()

# bikin koneksi ke db
conn_ehr = db_connection.create_connection(db_connection.replika_ehr)
conn_his = db_connection.create_connection(db_connection.replika_his)
conn_dwh_sqlserver = db_connection.create_connection(db_connection.dwh_sqlserver)

#ambil source
source_ehr = pd.read_sql_query(""" select 
                                    TRIM(a.bed_id) as BedID,
                                    TRIM(a.bed_ext_id) as BedCode,
                                    b.obj_nm as BedName,
                                    TRIM(a.location_id) as LocationID,
                                    a.condition_cd as ConditionCode,
                                    a.status_cd as StatusCode,
                                    a.updated_dttm as UpdateDateApp
                                from xocp_ehr_bed a 
                                left join xocp_ehr_obj b on a.bed_id = b.obj_id 
                                where
                                (a.created_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 3 DAY), "%%Y-%%m-%%d 00:00:00") AND
                                a.created_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 23:59:59")) 
                                OR (a.updated_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 3 DAY), "%%Y-%%m-%%d 00:00:00") AND
                                a.updated_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 23:59:59"))                    
                            """, conn_ehr)

print(source_ehr)

source_his = pd.read_sql_query(""" select 
                                    a.bed_id as BedID,
                                    a.bed_cd as BedCode,
                                    a.bed_nm as BedName,
                                    a.location_id as LocationID,
                                    a.bed_cond as ConditionCode,
                                    a.bed_status as StatusCode,
                                    a.updated_dttm as UpdateDateApp
                                from xocp_his_bed a 
                            """, conn_his)
print(source_his)
source_his['BedID']=source_his['BedID'].astype('str')
source_his['LocationID']=source_his['LocationID'].astype('str')

def find_data_insert_update(source, flag):
    if source.empty:
        print('tidak ada data dari source')
    else :
        if len(source) == 1:        
            # ambil primary key dari source, ambil index ke 0
            bedid = source["BedID"].values[0]
            # query buat narik data dari target lalu filter berdasarkan primary key
            query = f"SELECT BedID, BedCode,BedName,LocationID,ConditionCode,StatusCode,UpdateDateApp from dwhrscm_talend.DimBed where BedID IN ('{bedid}') order by BedID"
            target_filter = pd.read_sql_query(query, conn_dwh_sqlserver)
        else :
            # ambil primary key dari source, ambil index ke 0
            bedid = tuple(source["BedID"].unique())
            # query buat narik data dari target lalu filter berdasarkan primary key
            query = f'SELECT BedID, BedCode,BedName,LocationID,ConditionCode,StatusCode,UpdateDateApp from dwhrscm_talend.DimBed where BedID IN {bedid} order by BedID'
            target_filter = pd.read_sql_query(query, conn_dwh_sqlserver)
        
    # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
    change = source[~source.apply(tuple,1).isin(target_filter.apply(tuple,1))]

    # ambil data dari variable change ambil primary key BedID cek dengan variable target_filter, masukkan ke variable modified
    modified = change[change[['BedID']].apply(tuple,1).isin(target_filter[['BedID']].apply(tuple,1))]
    # tambahkan kolom ScdStart isi dengan tanggal, dan kolom ScdActive beri nilai 1
    modified['ScdStart']=date
    modified['ScdActive']=1
    modified['Flag'] = flag

    # ambil data dari variable change, ambil primary key BedID dan variable target_filter, masukkan ke variable inserted, kebalikan dari update pake simbol ~
    inserted =  change[~change[['BedID']].apply(tuple,1).isin(target_filter[['BedID']].apply(tuple,1))]
    inserted['ScdStart']=date
    inserted['ScdActive']=1
    inserted['Flag'] = flag
    return modified,inserted

# jalankan function FinDataInsertUpdate, masukkan source (ehr atau his) dan flag nya (1 atau 2) 
# lalu di assign ke variabel modified dan inserted

modified_ehr, inserted_ehr = find_data_insert_update(source_ehr,1)
modified_his, inserted_his = find_data_insert_update(source_his,2)

# gabungin dataframe yg modified dan inserted antara ehr dan his
modified_all = [modified_ehr,modified_his]
modified = pd.concat(modified_all,ignore_index=True)
print('ini modified')
print(modified)

inserted_all = [inserted_ehr,inserted_his]
inserted = pd.concat(inserted_all, ignore_index=True)
print('ini inserted')
print(inserted)

# buat fungsi untuk membuat ScdType 2 dan insert datanya
def create_surrogate_and_push(df):
    new_query = f'SELECT BedSurrogateKey FROM dwhrscm_talend.DimBed'
    new_target = pd.read_sql_query(new_query, conn_dwh_sqlserver)
    df.insert(0, 'BedSurrogateKey', range(len(new_target)+1, len(new_target)+1 + len(df)))
    df.to_sql('DimBed',schema='dwhrscm_talend',con=conn_dwh_sqlserver,if_exists='append',index=False)

#jika tidak ada data yang update, data yg new record langsung masuk ke table tujuan
if modified.empty:
    if inserted.empty:
        print('there is no new and updated data')
    else:
        create_surrogate_and_push(inserted)
        print('success add new record without any update')
else:
    #jika ada data yang update, lakukan proses update yg lama, baru masukan data yg modified dan inserted
    if len(modified['BedID']) == 1:
        bedid_update = modified['BedID'].values[0]
        query_update = f" UPDATE dwhrscm_talend.DimBed SET ScdEnd = '{date}', ScdActive = 0 WHERE BedID in ('{bedid_update}') and ScdActive =1"
        conn_dwh_sqlserver.execute(query_update)
        print('success update data existing')

    else :
        bedid_update = tuple(modified['BedID'])
        query_update = f"UPDATE dwhrscm_talend.DimBed SET ScdEnd = '{date}', ScdActive = 0 WHERE BedID in {bedid_update} and ScdActive =1"
        conn_dwh_sqlserver.execute(query_update)

    create_surrogate_and_push(modified)
    create_surrogate_and_push(inserted)  
    print('success insert new data and updated data')

#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)

text= f'scheduler tanggal : {date}'
print(text)
sys.stdout.close()

conn_ehr.close()
conn_his.close()
conn_dwh_sqlserver.close()