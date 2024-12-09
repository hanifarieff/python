import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pyodbc
import pandas as pd
import time
from datetime import timedelta
import datetime as dt
date = dt.datetime.today()

#bikin log ke file
sys.stdout = open("C:/TestPython/DWH_SQL_Server/Staging/logs/LogTransPrescriptionResponsTime.txt","w")
t0 = time.time()

#bikin koneksi ke db
conn_ehr = db_connection.create_connection(db_connection.replika_ehr)
conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)
conn_ehr_live = db_connection.create_connection(db_connection.ehr_live)

# buat variabel start_date dan end_date dengan mengambil 2 hari dan 1 hari ke belakang untuk dimasukkan di query
start_date = (date - timedelta(days=2)).strftime('%Y-%m-%d')
end_date = (date - timedelta(days=1)).strftime('%Y-%m-%d')

# start_date = f"2024-04-27"
# end_date = f"2024-04-27"

# buat query untuk menarik data dari source
query_source = f""" SELECT 
                    a.prescription_id PrescriptionID,
                    a.patient_id PatientID,
                    a.admission_id AdmissionID,
                    e.admission_dttm as AdmissionDate,
                    a.org_id as OrgID,
                    CASE
                        WHEN a.respontime_id IS NULL THEN '-'
                        ELSE a.respontime_id  
                    END AS ResponTimeID,
                    CASE
                        WHEN a.respontime_type IS NULL THEN '-'
                        ELSE a.respontime_type 
                    END AS ResponTimeType,
                    CASE
                        WHEN b.respontime_nm IS NULL THEN '-'
                        ELSE b.respontime_nm 
                    END AS ResponTimeName,
                    SUM(CASE WHEN c.obj_id != 'RACIKAN' THEN 1 ELSE 0 END) AS ItemAmountNonRacikan,
                    SUM(CASE WHEN c.obj_id = 'RACIKAN' THEN c.obj_qty ELSE 0 END) AS ItemAmountRacikan,
                    n.ordered_dttm as OrderDate,
                    a.dispensed_dttm DispenseDate,
                    a.prepared_dttm PreparedDate,
                    CASE
                        WHEN a.checked_dttm = '1000-01-01 00:00:00'  THEN NULL
                        ELSE a.checked_dttm 
                    END AS CheckedDate,
                    a.finished_dttm FinishedDate,
                    a.given_dttm GivenDate,
                    a.dispensed_user_id as DispenseUser,
                    a.prepared_user_id as PreparedUser,
                    a.checked_user_id as CheckedUser,
                    a.finished_user_id as FinishedUser,
                    a.given_user_id as GivenUser,
                    CASE
                        WHEN TIMEDIFF(a.finished_dttm,a.dispensed_dttm) LIKE '-%%' THEN '00:00:00'
                        ELSE TIMEDIFF(a.finished_dttm,a.dispensed_dttm) 
                    END AS TransactionTime
                -- DENSE_RANK()OVER(PARTITION BY a.patient_id, a.admission_id ORDER BY g.appointment_id ASC) RankByAppointment 
            FROM xocp_ehr_prescription_responstime a
            LEFT JOIN xocp_ehr_prescription_responstime_type b on a.respontime_id = b.respontime_id and a.respontime_type = b.respontime_type
            LEFT JOIN xocp_ehr_prescription_x_item c on a.prescription_id = c.prescription_id and c.status_cd = 'normal'
            LEFT JOIN xocp_ehr_patient_admission e on a.patient_id = e.patient_id and a.admission_id = e.admission_id
            -- LEFT JOIN xocp_ehr_patient f on a.patient_id = f.patient_id and e.status_cd = 'normal'
            LEFT JOIN xocp_ehr_prescription_x n on n.prescription_id = a.prescription_id
            WHERE
            (a.dispensed_dttm >= '{start_date} 00:00:00' AND a.dispensed_dttm <= '{end_date} 23:59:59') OR
            (a.updated_dttm >= '{start_date} 00:00:00' AND a.dispensed_dttm <= '{end_date} 23:59:59')
            -- AND a.patient_id = 1628708 and a.admission_id = 45
            -- (a.dispensed_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 2 DAY), "%%Y-%%m-%%d 00:00:00") AND a.dispensed_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 23:59:59"))
            -- OR (a.updated_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 00:00:00") AND a.updated_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 23:59:59"))
            -- AND a.prescription_id IN('00140001163449','00140000027806')
            GROUP BY
                    a.prescription_id,
                    a.patient_id,
                    a.admission_id,
                    e.admission_dttm,
                    -- f.patient_ext_id,
                    -- h.person_nm,
                    -- e.admission_dttm,
                    -- g.appointment_id,
                    a.respontime_type,
                    b.respontime_nm,
                    a.respontime_id,
                    n.ordered_dttm,
                    a.dispensed_dttm,
                    a.prepared_dttm,
                    a.checked_dttm,
                    a.finished_dttm,
                    a.given_dttm,
                    a.dispensed_user_id,
                    a.prepared_user_id,
                    a.finished_user_id,
                    a.checked_user_id,
                    a.given_user_id """

source = pd.read_sql_query(query_source,conn_ehr)
print(source)
source['ItemAmountNonRacikan'] = source['ItemAmountNonRacikan'].astype('int64')
source['ItemAmountRacikan'] = source['ItemAmountRacikan'].astype('int64')
source['DispenseUser'] = source['DispenseUser'].astype('str')
source['PreparedUser'] = source['PreparedUser'].astype('str')
source['FinishedUser'] = source['FinishedUser'].astype('str')
source['CheckedUser'] = source['CheckedUser'].astype('str')
source['GivenUser'] = source['GivenUser'].astype('str')
print(source.dtypes)

if source.empty:
    print('tidak ada data dari source')
else:
    # cek length variabel yang jadi primary key, jika isi nya cuma 1 maka hapus komanya
    prescriptionid = tuple(source["PrescriptionID"].unique())

    def remove_comma(x):
        if len(x) ==1:
            return f"{x[0]}"
        else:
            return x
    
    prescriptionid = remove_comma(prescriptionid)
        
    # query buat narik data dari target lalu filter berdasarkan primary key
    query = f""" SELECT 
                    TRIM(PrescriptionID) PrescriptionID,
                    PatientID,
                    AdmissionID,
                    OrgID,
                    ResponTimeID,
                    ResponTimeType,
                    ResponTimeName,
                    ItemAmountNonRacikan,
                    ItemAmountRacikan,
                    OrderDate,
                    DispenseDate,
                    PreparedDate,
                    CheckedDate,
                    FinishedDate,
                    GivenDate,
                    DispenseUser,
                    PreparedUser,
                    CheckedUser,
                    FinishedUser,
                    GivenUser,
                    CAST(TransactionTime as varchar(9)) as TransactionTime
                FROM staging_rscm.TransPrescriptionResponsTime
                WHERE PrescriptionID IN {prescriptionid} ORDER BY PrescriptionID"""
    target = pd.read_sql_query(query, conn_staging_sqlserver)
    print(target.dtypes)

    # print(source.iloc[:,0:3].apply(tuple,1).isin(target.iloc[:,0:3].apply(tuple,1)))
    # print(source.iloc[:,2:4].apply(tuple,1).isin(target.iloc[:,2:4].apply(tuple,1)))
    # print(source.iloc[:,3:5].apply(tuple,1).isin(target.iloc[:,3:5].apply(tuple,1)))
    # print(source.iloc[:,4:6].apply(tuple,1).isin(target.iloc[:,4:6].apply(tuple,1)))
    # print(source.iloc[:,5:7].apply(tuple,1).isin(target.iloc[:,5:7].apply(tuple,1)))
    # print(source.iloc[:,7:9].apply(tuple,1).isin(target.iloc[:,7:9].apply(tuple,1)))
    # print(source.iloc[:,9:11].apply(tuple,1).isin(target.iloc[:,9:11].apply(tuple,1)))
    # print(source.iloc[:,11:13].apply(tuple,1).isin(target.iloc[:,11:13].apply(tuple,1)))
    # print(source.iloc[:,13:15].apply(tuple,1).isin(target.iloc[:,13:15].apply(tuple,1)))
    # print(source.iloc[:,15:17].apply(tuple,1).isin(target.iloc[:,15:17].apply(tuple,1)))
    # print(source.iloc[:,17:20].apply(tuple,1).isin(target.iloc[:,17:20].apply(tuple,1)))
    # print(source.iloc[:,20:22].apply(tuple,1).isin(target.iloc[:,20:22].apply(tuple,1)))
    # print(source.iloc[:,23:24].apply(tuple,1).isin(target.iloc[:,23:24].apply(tuple,1)))
    # print(source.iloc[:,2:4].apply(tuple,1))
    # print(target.iloc[:,2:4].apply(tuple,1))
    # print(source.iloc[:,3:5].apply(tuple,1))
    # print(target.iloc[:,3:5].apply(tuple,1))
    # print(source.iloc[:,4:6].apply(tuple,1))
    # print(target.iloc[:,4:6].apply(tuple,1))
    # print(source.iloc[:,5:7].apply(tuple,1))
    # print(target.iloc[:,5:7].apply(tuple,1))
    # print(source.iloc[:,7:9].apply(tuple,1))
    # print(target.iloc[:,7:9].apply(tuple,1))
    # print(source.iloc[:,9:11].apply(tuple,1))
    # print(target.iloc[:,9:11].apply(tuple,1))
    # print(source.iloc[:,11:13].apply(tuple,1))
    # print(target.iloc[:,11:13].apply(tuple,1))
    # print(source.iloc[:,13:15].apply(tuple,1))
    # print(target.iloc[:,13:15].apply(tuple,1))
    # print(source.iloc[:,15:17].apply(tuple,1))
    # print(target.iloc[:,15:17].apply(tuple,1))
    # print(source.iloc[:,17:20].apply(tuple,1))
    # print(target.iloc[:,17:20].apply(tuple,1))
    # print(source.iloc[:,22:24].apply(tuple,1))
    # print(target.iloc[:,22:24].apply(tuple,1))
    # print('bates lagi')
    # print(source.iloc[:,21:25].apply(tuple,1))
    # print(target.iloc[:,21:25].apply(tuple,1))

    print(source.shape)
    print(target.shape)

    # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

    # ambil data yang update dari changes
    modified = changes[changes[['PrescriptionID']].apply(tuple,1).isin(target[['PrescriptionID']].apply(tuple,1))]
    total_row_upd = len(modified)
    text_upd = f'total row update : {total_row_upd}'
    print(text_upd)
    print(modified)

    # ambil data yang new dari changes
    inserted = changes[~changes[['PrescriptionID']].apply(tuple,1).isin(target[['PrescriptionID']].apply(tuple,1))]
    total_row_ins = len(inserted)
    text_ins = f'total row inserted : {total_row_ins}'
    print(text_ins)
    print(inserted)

    if modified.empty:
        # bikin tanggal sekarang buat kolom InsertDateStaging
        today = dt.datetime.now()
        today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
        inserted['InsertDateStaging'] = today_convert
        inserted.to_sql('TransPrescriptionResponsTime', schema='staging_rscm', con = conn_staging_sqlserver, if_exists = 'append', index=False)
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
            df.to_sql(temp_table,schema = 'staging_rscm',con=conn_staging_sqlserver, if_exists='replace',index=False)
            update_stmt_1 = f'UPDATE r '
            update_stmt_2 = f'SET '
            update_stmt_3 = ", ".join(list_col)
            update_stmt_8 = f' , r.UpdateDateStaging = CONVERT(DATETIME2(0), GETDATE(),120)'
            update_stmt_4 = f' FROM staging_rscm.{table} r '
            update_stmt_5 = f'INNER JOIN (SELECT * FROM staging_rscm.{temp_table}) as t ON r.{pk_1} = t.{pk_1} '
            update_stmt_6 = f'WHERE r.{pk_1} = t.{pk_1} '
            update_stmt_7 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_8 + update_stmt_4 + update_stmt_5 + update_stmt_6 +";"
            delete_stmt_1 = f'DROP TABLE staging_rscm.{temp_table}'
            print(update_stmt_7)
            conn_staging_sqlserver.execute(update_stmt_7)
            conn_staging_sqlserver.execute(delete_stmt_1)

        try:
            # update data
            updated_to_sql(modified, 'TransPrescriptionResponsTime', 'PrescriptionID')

            # insert data baru
            today = dt.datetime.now()
            today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
            inserted['InsertDateStaging'] = today_convert
            inserted.to_sql('TransPrescriptionResponsTime', schema='staging_rscm', con=conn_staging_sqlserver, if_exists ='append',index=False)
            print('success update and insert all data')
        
        except Exception as e:
            print(e)

#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)

text=f'scheduler tanggal : {date}'
print(text)


db_connection.close_connection(conn_ehr)
db_connection.close_connection(conn_staging_sqlserver)
db_connection.close_connection(conn_ehr_live)