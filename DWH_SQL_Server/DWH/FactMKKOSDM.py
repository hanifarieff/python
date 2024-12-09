import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pandas as pd
import numpy as np
import time
import datetime as dt
date = dt.datetime.today()

# bikin log ke file
sys.stdout = open("C:/TestPython/DWH_SQL_Server/DWH/logs/LogFactMKKOSDM.txt","w")
t0 = time.time()

# bikin koneksi ke db
conn_dwh_sqlserver = db_connection.create_connection(db_connection.dwh_sqlserver)
conn_ehr_live = db_connection.create_connection(db_connection.ehr_live)

source = pd.read_sql_query(""" SELECT
                                CURDATE() as TanggalKirim, 
                                CASE
                                    WHEN EmployeeGroup = 'MEDIS' THEN 'JumlahDokter'
                                    WHEN EmployeeGroup = 'PERAWAT' THEN 'JumlahPerawat'
                                    WHEN EmployeeGroup = 'KES LAIN' THEN 'JumlahPenunjang'
                                    WHEN EmployeeGroup = 'NON KES' THEN 'JumlahStaffAdministrasi'
                                    ELSE 'Tidak Diketahui'
                                END AS JenisPegawai
                                , COUNT(EmployeeID) Jumlah 
                                FROM VwEmployee10 
                                WHERE StatusCode = 'active'  
                                GROUP BY employeeGroup
                            """, conn_ehr_live)

print(source)

# # Pivot the DataFrame
# pivot_df = source.pivot(index='TanggalKirim', columns='EmployeeGroup', values='Jumlah').reset_index()

# # Rename columns to remove spaces (if necessary)
# pivot_df.columns.name = None  # Remove the name for the columns
# pivot_df.columns = ['TanggalKirim', 'JumlahDokter', 'JumlahPerawat', 'JumlahPenunjang', 'JumlahStaffAdministrasi']

# print(pivot_df)
# source=pivot_df

if source.empty:
    print('tidak ada data dari source')
else:
    source['TanggalKirim'] = pd.to_datetime(source.TanggalKirim, format='%Y-%m-%d')
    source['TanggalKirim'] = source['TanggalKirim'].dt.strftime('%Y-%m-%d')
    TanggalKirim = tuple(source["TanggalKirim"].unique())

    if len(TanggalKirim) > 1:
        pass
    else:
        TanggalKirim = str(TanggalKirim).replace(',','')
       
    # ambil primary key dari source, pake unique biar tidak duplicate    
    query = f'SELECT TanggalKirim,JenisPegawai,Jumlah FROM dwhrscm_talend.FactMKKOSDM WHERE TanggalKirim IN {TanggalKirim} ORDER BY TanggalKirim'
    target = pd.read_sql(query,conn_dwh_sqlserver)
    target['TanggalKirim'] = pd.to_datetime(target.TanggalKirim,format='%Y-%m-%d')
    target['TanggalKirim'] = target['TanggalKirim'].dt.strftime('%Y-%m-%d')

    print(target)

    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

    modified = changes[changes[['TanggalKirim','JenisPegawai']].apply(tuple,1).isin(target[['TanggalKirim','JenisPegawai']].apply(tuple,1))]
    total_row_upd = len(modified)
    text_upd = f'total row update : {total_row_upd}'
    print(text_upd)
    print(modified)

    # ambil data yang new dari changes
    inserted = changes[~changes[['TanggalKirim','JenisPegawai']].apply(tuple,1).isin(target[['TanggalKirim','JenisPegawai']].apply(tuple,1))]
    total_row_ins = len(inserted)
    text_ins = f'total row inserted : {total_row_ins}'
    print(text_ins)
    print(inserted)

    if modified.empty:
        inserted['IsSent'] = 0
        inserted.to_sql('FactMKKOSDM', schema='dwhrscm_talend',con=conn_dwh_sqlserver, if_exists='append',index=False)
        print('success insert all data without update')
    else:
        # buat fungsi untuk update data ke tabel target 
        def updated_to_sql(df, table_name,key_1,key_2):
            list_col = []
            table=table_name
            pk_1 = key_1
            pk_2 = key_2
            temp_table =f'{table}_temporary_table'
            for col in df.columns:
                if col == pk_1 or col == pk_2:
                    continue
                list_col.append(f'r.{col}=t.{col}')
            df.to_sql(temp_table,schema='dwhrscm_talend',con=conn_dwh_sqlserver, if_exists='replace',index=False)
            update_stmt_1 = f'UPDATE r '
            update_stmt_2 = f'SET '
            update_stmt_3 = ", ".join(list_col)
            update_stmt_4 = f' , r.IsSent = 0'
            update_stmt_5 = f' , r.UpdateDateDWH = CONVERT(DATETIME2(0), GETDATE(),120)'
            update_stmt_6 = f' FROM dwhrscm_talend.{table} r '
            update_stmt_7 = f'INNER JOIN (SELECT * FROM dwhrscm_talend.{temp_table}) as t ON r.{pk_1} = t.{pk_1} and r.{pk_2} = t.{pk_2} '
            update_stmt_8 = f' WHERE r.{pk_1} = t.{pk_1} and r.{pk_2} = t.{pk_2} '
            update_stmt_9 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_4 + update_stmt_5 + update_stmt_6 + update_stmt_7 + update_stmt_8 + ";"
            delete_stmt_1 = f'DROP TABLE dwhrscm_talend.{temp_table}'
            print(update_stmt_9)
            conn_dwh_sqlserver.execute(update_stmt_9)
            conn_dwh_sqlserver.execute(delete_stmt_1)
        
        try:
            #update data
            updated_to_sql(modified, 'FactMKKOSDM', 'TanggalKirim','JenisPegawai')

            #insert data baru
            inserted['IsSent'] = 0
            inserted.to_sql('FactMKKOSDM', schema='dwhrscm_talend',con=conn_dwh_sqlserver, if_exists='append',index=False)
            print('success update and insert all data')
        except Exception as e:
            print(e)

#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)

text=f'scheduler tanggal : {date}'
print(text)

db_connection.close_connection(conn_dwh_sqlserver)
db_connection.close_connection(conn_dwh_sqlserver)