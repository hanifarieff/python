import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection

import pandas as pd
import time
import datetime as dt
date = dt.datetime.today()

# bikin log output ke file 
sys.stdout = open("C:/TestPython/DWH_SQL_Server/Staging/logs/LogTransRadiologyReport.txt","w")

t0 = time.time()

# koneksi ke db
conn_ehr_live= db_connection.create_connection(db_connection.ehr_live)
conn_staging_sqlserver=db_connection.create_connection(db_connection.staging_sqlserver)

source = pd.read_sql_query(""" 
                               SELECT 
                                    MAX(a.report_id) as ReportID,
                                    a.MedicalNo,
                                    a.AccessionNumber,
                                    a.OrderID,
                                    a.OrderDate,
                                    a.ObservationDate,
                                    a.ServiceID,
                                    a.ServiceName,
                                    a.Report,
                                    CASE
                                        WHEN a.responsible_observer_id is not null THEN a.responsible_observer_id 
                                        ELSE a.responsible_observer_id_addendum
                                    END AS ResponsibleObserverID,
                                    CASE 
                                        WHEN a.responsible_observer_nm is not null then a.responsible_observer_nm
                                        ELSE a.responsible_observer_nm_addendum
                                    END AS ResponsibleObserverName,
                                    a.IsAddendum
                                FROM
                                (SELECT
                                    report_id,
                                    CONCAT_WS('-',SUBSTRING(mrn,1,3),SUBSTRING(mrn,4,2),SUBSTRING(mrn,6,2)) as MedicalNo,
                                    REGEXP_SUBSTR(accession_number,'[A-Z]+') as AccessionNumber,
                                    REGEXP_SUBSTR(accession_number,'[0-9]+') as OrderID,
                                    CAST(order_dttm as datetime) as OrderDate,
                                    CAST(observation_dttm as datetime) as ObservationDate,
                                service_id as ServiceID,
                                    service_nm as ServiceName,
                                    CASE
                                        WHEN report_txt is not null THEN report_txt
                                        ELSE report_txt_addendum
                                    END AS Report,
                                    responsible_observer_id,
                                    responsible_observer_id_addendum,
                                    responsible_observer_nm,
                                    responsible_observer_nm_addendum,
                                    CASE
                                        WHEN responsible_observer_id_addendum is null THEN 'n'
                                        ELSE 'y'
                                    END AS IsAddendum
                                from radiology_report
                                -- where mrn = 4244055 and accession_number = 'R00200000061969'
                                -- -- where mrn = 4620761 and accession_number = 'CT00190003779023'
                                -- OR mrn = 4585800 and accession_number = 'GD00190005084958'
                                -- OR mrn = 4463811 and accession_number = 'MR00190002686162'
                                -- WHERE DATE_FORMAT(order_dttm,'%%Y-%%m-%%d %%h:%%i:%%s') >= '2024-03-01 00:00:00' 
                                -- AND DATE_FORMAT(order_dttm,'%%Y-%%m-%%d %%h:%%i:%%s') <= '2024-04-23 23:59:59' 
                                WHERE 
                                -- order_dttm >= '20241201000000' and order_dttm <= '20241231235959'
                                (DATE_FORMAT(order_dttm,'%%Y-%%m-%%d %%h:%%i:%%s') >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 4 DAY), "%%Y-%%m-%%d 00:00:00")
                                AND DATE_FORMAT(order_dttm,'%%Y-%%m-%%d %%h:%%i:%%s') <= DATE_FORMAT(NOW(), "%%Y-%%m-%%d 23:59:59"))
                                OR 
                                (DATE_FORMAT(observation_dttm,'%%Y-%%m-%%d %%h:%%i:%%s') >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 4 DAY), "%%Y-%%m-%%d 00:00:00")
                                AND DATE_FORMAT(observation_dttm,'%%Y-%%m-%%d %%h:%%i:%%s') <= DATE_FORMAT(NOW(), "%%Y-%%m-%%d 23:59:59"))
                                -- ORDER BY REGEXP_SUBSTR(accession_number,'[0-9]+'), CAST(observation_dttm as datetime)
                                ) a
                                GROUP BY 
                                    a.MedicalNo,
                                    a.AccessionNumber,
                                    a.OrderID,
                                    a.OrderDate,
                                    a.ObservationDate,
                                    a.ServiceID,
                                    a.ServiceName,
                                    a.Report,
                                    a.responsible_observer_id,
                                    a.responsible_observer_nm,
                                    a.responsible_observer_id_addendum,
                                    a.responsible_observer_nm_addendum
                                ORDER BY a.ObservationDate 
                            """, conn_ehr_live)
print(source)

if source.empty:
    print('tidak ada data dari source')
else:
    # jika dari source cuma 1 row
    if len(source) == 1:        
        # ambil primary key dari source, ambil index ke 0
        reportid = source["ReportID"].values[0]

        # query buat narik data dari target lalu filter berdasarkan primary key
        query = f'SELECT ReportID,MedicalNo,AccessionNumber,OrderID,OrderDate,ObservationDate,ServiceID,ServiceName,Report,ResponsibleObserverID,ResponsibleObserverName,IsAddendum FROM staging_rscm.TransRadiologyReport WHERE ReportID IN ({reportid}) ORDER BY OrderDate'
        target = pd.read_sql_query(query, conn_staging_sqlserver)
    else :
         # ambil primary key dari source, pake unique biar tidak duplicate
        reportid = tuple(source["ReportID"].unique())

         # query buat narik data dari target lalu filter berdasarkan primary key
        query = f'SELECT ReportID,MedicalNo,AccessionNumber,OrderID,OrderDate,ObservationDate,ServiceID,ServiceName,Report,ResponsibleObserverID,ResponsibleObserverName,IsAddendum FROM staging_rscm.TransRadiologyReport WHERE ReportID IN {reportid} ORDER BY OrderDate'
        target = pd.read_sql_query(query, conn_staging_sqlserver)
    print(target)
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
        # bikin tanggal sekarang buat kolom InsertDateStaging
        today = dt.datetime.now()
        today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
        inserted['InsertDateStaging'] = today_convert
        inserted.to_sql('TransRadiologyReport',schema='staging_rscm', con=conn_staging_sqlserver, if_exists = 'append', index=False)
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
            df.to_sql(temp_table,con=conn_staging_sqlserver,schema='staging_rscm', if_exists='replace',index=False)
            update_stmt_1 = f'UPDATE r '
            update_stmt_2 = f'SET '
            update_stmt_3 = ", ".join(list_col)
            update_stmt_4 = f' , r.UpdateDateStaging = CONVERT(DATETIME2(0), GETDATE(),120)'
            update_stmt_5 = f' FROM staging_rscm.{table} r '
            update_stmt_6 = f'INNER JOIN (SELECT * FROM staging_rscm.{temp_table}) as t ON r.{pk_1} = t.{pk_1} '
            update_stmt_7 = f' WHERE r.{pk_1} = t.{pk_1} '
            update_stmt_8 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_4 + update_stmt_5 + update_stmt_6 + update_stmt_7 + ";"
            delete_stmt_1 = f'DROP TABLE staging_rscm.{temp_table}'
            print(update_stmt_8)
            conn_staging_sqlserver.execute(update_stmt_8)
            conn_staging_sqlserver.execute(delete_stmt_1)

        try:
            # update data
            updated_to_sql(modified, 'TransRadiologyReport', 'ReportID')

            # insert data baru
            today = dt.datetime.now()
            today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
            inserted['InsertDateStaging'] = today_convert
            inserted.to_sql('TransRadiologyReport',schema='staging_rscm', con=conn_staging_sqlserver, if_exists ='append',index=False)
            print('success update and insert all data')
        
        except Exception as e:
            print(e)

#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)

text=f'scheduler tanggal : {date}'
print(text)

db_connection.close_connection(conn_ehr_live)
db_connection.close_connection(conn_staging_sqlserver)
sys.stdout.close()

