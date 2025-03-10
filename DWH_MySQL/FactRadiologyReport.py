from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine
import pandas as pd
import time
import datetime as dt
date = dt.datetime.today()
import sys

sys.stdout = open("C:/TestPython/DWH_MySQL/logs/LogFactPatientRadiologyReport.txt","w")

t0 = time.time()
ehr = create_engine('mysql://hanif-ppi:hanif2022@172.16.19.11/ehr')
dwh_talend = create_engine('mysql://hanif-ppi:hanif2022@172.16.5.33/dwhrscm_talend')

try:
    conn_ehr = ehr.connect()
    conn_dwh = dwh_talend.connect()
    print('successfully connect DB')
except SQLAlchemyError as e:
    error = str(e.__dict__['orig'])
    print(error)

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
                                -- WHERE DATE_FORMAT(order_dttm,'%%Y-%%m-%%d %%h:%%i:%%s') >= '2023-07-01 00:00:00' 
                                -- AND DATE_FORMAT(order_dttm,'%%Y-%%m-%%d %%h:%%i:%%s') <= '2023-07-15 23:59:59' 
                                WHERE
                                -- accession_number = 'R00200003451463'
                                (DATE_FORMAT(order_dttm,'%%Y-%%m-%%d %%h:%%i:%%s') >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 4 DAY), "%%Y-%%m-%%d 00:00:00")
                                AND DATE_FORMAT(order_dttm,'%%Y-%%m-%%d %%h:%%i:%%s') <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 23:59:59"))
                                OR 
                                (DATE_FORMAT(observation_dttm,'%%Y-%%m-%%d %%h:%%i:%%s') >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 4 DAY), "%%Y-%%m-%%d 00:00:00")
                                AND DATE_FORMAT(observation_dttm,'%%Y-%%m-%%d %%h:%%i:%%s') <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 23:59:59"))
                                ORDER BY REGEXP_SUBSTR(accession_number,'[0-9]+'), CAST(observation_dttm as datetime)
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
                            """, conn_ehr)
print(source)

if source.empty:
    print('tidak ada data dari source')
else:
    # jika dari source cuma 1 row
    if len(source) == 1:        
        # ambil primary key dari source, ambil index ke 0
        reportid = source["ReportID"].values[0]

        # query buat narik data dari target lalu filter berdasarkan primary key
        query = f'SELECT ReportID,MedicalNo,AccessionNumber,OrderID,OrderDate,ObservationDate,ServiceID,ServiceName,Report,ResponsibleObserverID,ResponsibleObserverName,IsAddendum FROM FactRadiologyReport WHERE ReportID IN ({reportid}) ORDER BY OrderDate'
        target = pd.read_sql_query(query, conn_dwh)
    else :
         # ambil primary key dari source, pake unique biar tidak duplicate
        reportid = tuple(source["ReportID"].unique())

         # query buat narik data dari target lalu filter berdasarkan primary key
        query = f'SELECT ReportID,MedicalNo,AccessionNumber,OrderID,OrderDate,ObservationDate,ServiceID,ServiceName,Report,ResponsibleObserverID,ResponsibleObserverName,IsAddendum FROM FactRadiologyReport WHERE ReportID IN {reportid} ORDER BY OrderDate'
        target = pd.read_sql_query(query, conn_dwh)
    print(target)
    # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

    # ambil data yang update dari changes
    modified = changes[changes[['ReportID']].apply(tuple,1).isin(target[['ReportID']].apply(tuple,1))]
    total_row_upd = len(modified)
    text_upd = f'total row update : {total_row_upd}'
    print(text_upd)
    print(modified.iloc[:,[0,1,2,4,5,]])


    # ambil data yang new dari changes
    inserted = changes[~changes[['ReportID']].apply(tuple,1).isin(target[['ReportID']].apply(tuple,1))]
    total_row_ins = len(inserted)
    text_ins = f'total row inserted : {total_row_ins}'
    print(text_ins)
    print(inserted.iloc[:,[0,1,2,4,5]])

    if modified.empty:
        # bikin tanggal sekarang buat kolom InsertDateDWH
        # today = dt.datetime.now()
        # today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
        # inserted['InsertDateDWH'] = today_convert
        inserted.to_sql('FactRadiologyReport', con=conn_dwh, if_exists = 'append', index=False)
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
            df.to_sql(temp_table,con=conn_dwh, if_exists='replace',index=False)
            update_stmt_1 = f'UPDATE {table} r '
            update_stmt_2 = f'INNER JOIN (SELECT * FROM {temp_table}) as t ON r.{pk_1} = t.{pk_1} '
            update_stmt_3 = f'SET '
            update_stmt_4 = ", ".join(list_col)
            update_stmt_5 = f' WHERE r.{pk_1} = t.{pk_1} '
            update_stmt_6 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_4 + update_stmt_5 +";"
            delete_stmt_1 = f'DROP TABLE {temp_table}'
            print(update_stmt_6)
            conn_dwh.execute(update_stmt_6)
            conn_dwh.execute(delete_stmt_1)

        try:
            # update data
            updated_to_sql(modified, 'FactRadiologyReport', 'ReportID')

            # insert data baru
            # today = dt.datetime.now()
            # today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
            # inserted['InsertDateDWH'] = today_convert
            inserted.to_sql('FactRadiologyReport', con=conn_dwh, if_exists ='append',index=False)
            print('success update and insert all data')
        
        except Exception as e:
            print(e)

#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)

text=f'scheduler tanggal : {date}'
print(text)

conn_dwh.close()
conn_ehr.close()
sys.stdout.close()

