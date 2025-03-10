import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pandas as pd
import time
import datetime as dt
date = dt.datetime.today()

sys.stdout = open("C:/TestPython/DWH_SQL_Server/DWH/logs/LogDimDrugObject.txt","w")
t0 = time.time()

#bikin koneksi ke db
conn_ehr =  db_connection.create_connection(db_connection.replika_ehr)
conn_dwh_sqlserver = db_connection.create_connection(db_connection.dwh_sqlserver)

source = pd.read_sql_query(""" 
                                SELECT DISTINCT
                                        a.id AS ID,
                                        a.obj_id as ObjID,
                                        a.kfa_id as KfaID,
                                        b.kfa_code as KfaCode,
                                        b.kfa_nm as KfaName,
                                        a.status_cd as StatusCode,
                                        a.created_user_id as CreatedUserID,
                                        a.created_dttm as CreatedDate,
                                        a.update_user_id as UpdateUserID,
                                        updated_dttm as UpdateDate
                                        FROM xocp_inv_obj_kfa a
                                        INNER JOIN xocp_kfa_master b ON a.kfa_id = b.kfa_id
                                where status_cd <> 'duplicate' 
                                AND 
                                (a.updated_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 8 DAY), "%%Y-%%m-%%d 00:00:00") AND 
                                a.updated_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 23:59:59"))
                            """, conn_ehr)
print(source)

# try:
#     # today = dt.datetime.now()
#     # today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
#     # source['InsertDateDWH'] = today_convert
#     source.to_sql('DimObjectDrug', con=conn_dwh_sqlserver_sqlserver, if_exists = 'append', index=False)
#     print('success insert all data without update')
# except Exception as e:
#     print(e)

if source.empty:
    print('tidak ada data dari source')
else:
    # jika dari source cuma 1 row
    if len(source) == 1:        
        # ambil primary key dari source, ambil index ke 0
        ID = source["ID"].values[0]

        # query buat narik data dari target lalu filter berdasarkan primary key
        query = f'SELECT ID,TRIM(ObjID) as ObjID,KfaID,KfaCode,StatusCode,CreatedUserID,CreatedDate,UpdateUserID,UpdateDate from dwhrscm_talend.DimObjectDrug where ID IN ({ID})'
        target = pd.read_sql_query(query, conn_dwh_sqlserver)
    else :
         # ambil primary key dari source, pake unique biar tidak duplicate
        ID = tuple(source["ID"].unique())

         # query buat narik data dari target lalu filter berdasarkan primary key
        query = f'SELECT ID,TRIM(ObjID) as ObjID,KfaID,KfaCode,KfaName,StatusCode,CreatedUserID,CreatedDate,UpdateUserID,UpdateDate from dwhrscm_talend.DimObjectDrug where ID IN {ID}'
        target = pd.read_sql_query(query, conn_dwh_sqlserver)
    print(source.dtypes)
    print(target.dtypes)

    # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

    # ambil data yang update dari changes
    modified = changes[changes[['ID']].apply(tuple,1).isin(target[['ID']].apply(tuple,1))]
    total_row_upd = len(modified)
    text_upd = f'total row update : {total_row_upd}'
    print(text_upd)
    print(modified)

    # ambil data yang new dari changes
    inserted = changes[~changes[['ID']].apply(tuple,1).isin(target[['ID']].apply(tuple,1))]
    total_row_ins = len(inserted)
    text_ins = f'total row inserted : {total_row_ins}'
    print(text_ins)
    print(inserted)

    if modified.empty:
        # bikin tanggal sekarang buat kolom InsertDateDWH
        today = dt.datetime.now()
        today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
        inserted['InsertDateDWH'] = today_convert
        inserted.to_sql('DimObjectDrug', schema = 'dwhrscm_talend',con=conn_dwh_sqlserver, if_exists = 'append', index=False)
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
            update_stmt_4 = f' , r.UpdateDateDWH = CONVERT(DATETIME2(0), GETDATE(),120) '
            update_stmt_5= f' FROM dwhrscm_talend.{table} r '
            update_stmt_6 = f'INNER JOIN (SELECT * FROM dwhrscm_talend.{temp_table}) as t ON r.{pk_1} = t.{pk_1} '
            update_stmt_7 = f' WHERE r.{pk_1} = t.{pk_1} '
            update_stmt_8 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_4 + update_stmt_5 + update_stmt_6 + update_stmt_7 + ";"
            delete_stmt_1 = f'DROP TABLE dwhrscm_talend.{temp_table}'
            print(update_stmt_6)
            conn_dwh_sqlserver.execute(update_stmt_8)
            conn_dwh_sqlserver.execute(delete_stmt_1)

        try:
            # update data
            updated_to_sql(modified, 'DimObjectDrug', 'ID')

            # insert data baru
            today = dt.datetime.now()
            today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
            inserted['InsertDateDWH'] = today_convert
            inserted.to_sql('DimObjectDrug', schema = 'dwhrscm_talend',con=conn_dwh_sqlserver, if_exists ='append',index=False)
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
db_connection.close_connection(conn_ehr)

sys.stdout.close()

