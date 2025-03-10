import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pandas as pd
import time
import datetime as dt
date = dt.datetime.today()

# bikin log ke file
sys.stdout = open("C:/TestPython/DWH_SQL_Server/DWH/logs/LogFactRadiologyReport.txt","w")

t0 = time.time()

# bikin koneksi ke db
conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)
conn_dwh_sqlserver = db_connection.create_connection(db_connection.dwh_sqlserver)

source = pd.read_sql_query(""" 
                               SELECT 
                                    ReportID,
                                    AccessionNumber,
                                    OrderID,
                                    MedicalNo,
                                    OrderDate,
                                    ObservationDate,
                                    ServiceID,
                                    ServiceName,
                                    Report,
                                    ResponsibleObserverID,
                                    ResponsibleObserverName,
                                    IsAddendum
                                FROM staging_rscm.TransRadiologyReport a
                                WHERE 
                                -- OrderDate >= '2024-12-01 00:00:00' and OrderDate <= '2024-12-31 23:59:59'
                                -- ((CAST(InsertDateStaging as date) >= '2024-04-25 00:00:00' AND CAST(InsertDateStaging as date) <= '2024-04-25 23:59:00') OR
                                -- (CAST(UpdateDateStaging as date) >= '2024-04-25 00:00:00' AND CAST(UpdateDateStaging as date) <= '2024-04-25 23:59:00'))
                                ((CAST(InsertDateStaging as date) >= CAST(DATEADD(DAY, -1, GETDATE()) as date) AND CAST(InsertDateStaging as date) <= CAST(GETDATE() as date)) OR
                                (CAST(UpdateDateStaging as date) >= CAST(DATEADD(DAY, -1, GETDATE()) as date) AND CAST(UpdateDateStaging as date) <= CAST(GETDATE() as date)))
                                -- AND ReportID IN (1381934,1381935)
                            """, conn_staging_sqlserver)
print(source)

if source.empty:
    print('tidak ada data dari source')
else:
    # jika dari source cuma 1 row
    if len(source) == 1:        
        # ambil primary key dari source, ambil index ke 0
        reportid = source["ReportID"].values[0]

        # query buat narik data dari target lalu filter berdasarkan primary key
        query = f'SELECT ReportID,AccessionNumber,OrderID,MedicalNo,OrderDate,ObservationDate,ServiceID,ServiceName,Report,ResponsibleObserverID,ResponsibleObserverName,IsAddendum FROM dwhrscm_talend.FactRadiologyReport WHERE ReportID IN ({reportid}) ORDER BY OrderDate'
        target = pd.read_sql_query(query, conn_dwh_sqlserver)
    else :
         # ambil primary key dari source, pake unique biar tidak duplicate
        reportid = tuple(source["ReportID"].unique())

         # query buat narik data dari target lalu filter berdasarkan primary key
        query = f'SELECT ReportID,AccessionNumber,OrderID,MedicalNo,OrderDate,ObservationDate,ServiceID,ServiceName,Report,ResponsibleObserverID,ResponsibleObserverName,IsAddendum FROM dwhrscm_talend.FactRadiologyReport WHERE ReportID IN {reportid} ORDER BY OrderDate'
        target = pd.read_sql_query(query, conn_dwh_sqlserver)
    print(target)

    # print(source.iloc[:,0:3].apply(tuple,1))
    # print(target.iloc[:,0:3].apply(tuple,1))
    # print(source.iloc[:,2:4].apply(tuple,1))
    # print(target.iloc[:,2:4].apply(tuple,1))
    # print(source.iloc[:,3:5].apply(tuple,1))
    # print(target.iloc[:,3:5].apply(tuple,1))
    # print(source.iloc[:,4:6].apply(tuple,1))
    # print(target.iloc[:,4:6].apply(tuple,1))
    # print(source.iloc[:,5:7].apply(tuple,1))
    # print(target.iloc[:,5:7].apply(tuple,1))
    # print('bates')
    # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

    # ambil data yang update dari changes
    modified = changes[changes[['ReportID']].apply(tuple,1).isin(target[['ReportID']].apply(tuple,1))]
    total_row_upd = len(modified)
    text_upd = f'total row update : {total_row_upd}'
    print(text_upd)
    print(modified.iloc[:,[0,1,2,3,4,5,6,7]])
    print(modified.iloc[:,7:])

    # ambil data yang new dari changes
    inserted = changes[~changes[['ReportID']].apply(tuple,1).isin(target[['ReportID']].apply(tuple,1))]
    total_row_ins = len(inserted)
    text_ins = f'total row inserted : {total_row_ins}'
    print(text_ins)
    print(inserted)

    if modified.empty:
        # bikin tanggal sekarang buat kolom InsertedDateDWH
        today = dt.datetime.now()
        today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
        inserted['InsertedDateDWH'] = today_convert
        inserted.to_sql('FactRadiologyReport',schema='dwhrscm_talend', con=conn_dwh_sqlserver, if_exists = 'append', index=False)
        print('success insert all data without update')
    elif modified.empty and inserted.empty:
        print('tidak data yang update dan baru')
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
            df.to_sql(temp_table,schema='dwhrscm_talend',con=conn_dwh_sqlserver, if_exists='replace',index=False)
            update_stmt_1 = f'UPDATE r '
            update_stmt_2 = f'SET '
            update_stmt_3 = ", ".join(list_col)
            update_stmt_4 = f' , r.UpdateDateDWH = CONVERT(DATETIME2(0), GETDATE(),120) '
            update_stmt_5 = f'FROM dwhrscm_talend.{table} r '
            update_stmt_6 = f'INNER JOIN (SELECT * FROM dwhrscm_talend.{temp_table}) as t ON r.{pk_1} = t.{pk_1} '
            update_stmt_7 = f' WHERE r.{pk_1} = t.{pk_1} '
            update_stmt_8 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_4 + update_stmt_5 + update_stmt_6 + update_stmt_7 + ";"
            delete_stmt_1 = f'DROP TABLE dwhrscm_talend.{temp_table}'
            print(update_stmt_8)
            conn_dwh_sqlserver.execute(update_stmt_8)
            conn_dwh_sqlserver.execute(delete_stmt_1)

        try:
            # update data
            updated_to_sql(modified, 'FactRadiologyReport', 'ReportID')

            # insert data baru
            today = dt.datetime.now()
            today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
            inserted['InsertedDateDWH'] = today_convert
            inserted.to_sql('FactRadiologyReport',schema='dwhrscm_talend', con=conn_dwh_sqlserver, if_exists ='append',index=False)
            print('success update and insert all data')
        
        except Exception as e:
            print(e)

#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)

text=f'scheduler tanggal : {date}'
print(text)

db_connection.close_connection(conn_staging_sqlserver)
db_connection.close_connection(conn_dwh_sqlserver)
# conn_ehr.close()
sys.stdout.close()

