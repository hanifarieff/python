""" File ini berfungsi untuk menarik data `Respontime Obat Farmasi` di menu Laporan Respontime, Group Apoteker """

import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pyodbc
import pandas as pd
import time
from datetime import timedelta
import datetime as dt
date = dt.datetime.today()
import os
import psutil

#bikin log ke file
sys.stdout = open("C:/TestPython/DWH_SQL_Server/Staging/logs/LogTransPrescriptionResponsTime.txt","w")

# Get the current process info
process = psutil.Process(os.getpid())
""" Info code process"""

# Measure memory usage before code execution
memory_before = process.memory_info().rss / (1024 * 1024)  # In MB
""" Hitung Memori Awal sebelum code running"""
print(f"Memory before: {memory_before} MB")

t0 = time.time()
""" Waktu awal sebelum code running"""

def get_connections():
    """ Membuat koneksi ke database """
    conn_ehr = db_connection.create_connection(db_connection.replika_ehr)
    conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)
    conn_ehr_live = db_connection.create_connection(db_connection.ehr_live)
    return conn_ehr, conn_staging_sqlserver,conn_ehr_live

def get_source_data(conn_ehr):
    """ Extract data database sumber (EHR) """
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
    
    source = pd.read_sql_query(query_source, conn_ehr)    

    # Transform beberapa kolom menyesuaikan tipe data yang ada di tabel target 
    source['ItemAmountNonRacikan'] = source['ItemAmountNonRacikan'].astype('int64')
    source['ItemAmountRacikan'] = source['ItemAmountRacikan'].astype('int64')
    source['DispenseUser'] = source['DispenseUser'].astype('str')
    source['PreparedUser'] = source['PreparedUser'].astype('str')
    source['FinishedUser'] = source['FinishedUser'].astype('str')
    source['CheckedUser'] = source['CheckedUser'].astype('str')
    source['GivenUser'] = source['GivenUser'].astype('str')
    return source

def get_target_data(source,conn_staging_sqlserver):
    """ ambil data dari tabel target, yaitu TransPrescriptionResponsTime """

    # ambil value dari kolom PrescriptionID, untuk dimasukkan ke WHERE query target
    prescriptionid = tuple(source["PrescriptionID"].unique())

    def remove_comma(x):
        if len(x) ==1:
            return f"{x[0]}"
        else:
            return x
    
    prescriptionid = remove_comma(prescriptionid)

    query_target = f""" SELECT 
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
    target = pd.read_sql_query(query_target,conn_staging_sqlserver)
    return target
   
def detect_changes(source, target):
    """ deteksi perubahan antara dataframe `source` dan `target` """
    
    # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

    # ambil data yang update
    modified = changes[changes[['PrescriptionID']].apply(tuple,1).isin(target[['PrescriptionID']].apply(tuple,1))]

    # ambil data yang baru
    inserted = changes[~changes[['PrescriptionID']].apply(tuple,1).isin(target[['PrescriptionID']].apply(tuple,1))]

    return modified, inserted

def updated_data(df, table_name, key_1, conn_staging_sqlserver):
    """ update data di tabel target yaitu TransPrescriptionResponsTime"""
    if not df.empty:
        a = [f't.{col} = s.{col}' for col in df.columns if col != key_1 ]
        temp_table = f'{table_name}_temporary_table'
        
        # Upload the temp table to SQL Server
        df.to_sql(temp_table, schema='staging_rscm', con=conn_staging_sqlserver, if_exists = 'replace', index = False)
        update_stmt = (
            f'UPDATE t SET ' + ", ".join(a) +
            f', t.UpdateDateStaging = CONVERT(DATETIME2(0), GETDATE(),120)'
            f' FROM staging_rscm.{table_name} t '
            f'INNER JOIN (SELECT * FROM staging_rscm.{temp_table}) AS s ON t.{key_1} = s.{key_1} '
            f'WHERE t.{key_1} = s.{key_1};'
        )
        delete_stmt = f'DROP TABLE staging_rscm.{temp_table};'
        
        with conn_staging_sqlserver.begin() as transaction:
            # Execute update and delete temp table
            conn_staging_sqlserver.execute(update_stmt)
            conn_staging_sqlserver.execute(delete_stmt)
            print('\nData Success Updated')
    else:
        print('\nTidak ada data yang berubah')   

def inserted_data(inserted, conn_staging_sqlserver):
    """ insert data di tabel target """
    if not inserted.empty:
        with conn_staging_sqlserver.begin() as transaction:
            today = dt.datetime.now()
            today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
            inserted['InsertDateStaging'] = today_convert
            inserted.to_sql('TransPrescriptionResponsTime', schema='staging_rscm', con=conn_staging_sqlserver, if_exists='append', index=False)
            print('Data Success Inserted')
    else:
        print('Tidak ada data yang baru\n')

def main():
    """ Fungsi utama untuk menjalankan semua proses"""

    conn_ehr, conn_staging_sqlserver,conn_ehr_live = get_connections()

    try:
        # Ambil data dari source
        source = get_source_data(conn_ehr)
        print("Source Data:")
        print(source)

        # Ambil data dari target 
        target = get_target_data(source, conn_staging_sqlserver)
        print("Target Data:")
        print(target)

        # Deteksi perubahan (buat dapetin modified dan inserted)
        modified, inserted = detect_changes(source, target)
        print("Changes Detected:")
        print("Modified Data:")
        print(modified)
        print("Inserted Data:")
        print(inserted)

        # update data
        updated_data(modified, 'TransPrescriptionResponsTime', 'PrescriptionID', conn_staging_sqlserver)

        # insert data
        inserted_data(inserted, conn_staging_sqlserver)

    finally:
        db_connection.close_connection(conn_ehr)
        db_connection.close_connection(conn_staging_sqlserver)
        db_connection.close_connection(conn_ehr_live)

# Run the main process
if __name__ == "__main__":
    main()
     
#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
""" Kecepatan eksekusi program"""
print(total)
print('\n')

# Measure memory usage after code execution
memory_after = process.memory_info().rss / (1024 * 1024)  # In MB
""" Memory setelah di code selesai running"""
print(f"Memory after: {memory_after} MB")

# Calculate memory used
memory_used = memory_after - memory_before
""" Total Memori yang terpakai untuk menjalankan program ini"""
print(f"Memory used: {memory_used} MB")

text = dt.datetime.today()
""" tanggal hari ini """
print(f"scheduler tanggal : {text}")