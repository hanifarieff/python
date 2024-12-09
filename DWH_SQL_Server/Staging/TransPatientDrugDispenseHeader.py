import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pyodbc
import pandas as pd
import time
import datetime as dt
from datetime import timedelta
date = dt.datetime.today()
import os
import psutil

#bikin log ke file
sys.stdout = open("C:/TestPython/DWH_SQL_Server/Staging/logs/LogTransPatientDrugDispenseHeader.txt","w")

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
    # start_date = (date - timedelta(days=2)).strftime('%Y-%m-%d')
    # end_date = (date - timedelta(days=1)).strftime('%Y-%m-%d')

    start_date = f"2024-11-17"
    end_date = f"2024-11-17"

    # buat query untuk menarik data dari source
    query_source = f""" SELECT
                            a.prescription_id as PrescriptionID,
                            a.order_id as OrderID,
                            a.patient_id as PatientID, 
                            a.admission_id as AdmissionID,
                            b.patient_ext_id as MedicalNo,
                            a.payplan_id as PayplanID,
                            a.depo_org_id as DepoOrgID,
                            a.order_dttm as OrderDate,
                            a.status_cd as Status,
                            a.created_user_id as CreatedUserID,
                            a.billing_id as BillingID,
                            a.billing_paid as BillingPaid,
                            CASE
                                WHEN a.payplan_id = '8' THEN CAST(a.patient_pay as DECIMAL(30,2))
                                ELSE CAST(a.payor_pay as DECIMAL(30,2))
                            END AS BillingAmount
                        FROM xocp_ehr_ff_order a
                        left JOIN xocp_ehr_patient b on a.patient_id = b.patient_id
                        WHERE
                        a.order_dttm >= '{start_date} 00:00:00' and a.order_dttm <= '{end_date} 23:59:59'
                        -- and a.status_cd != 'nullified' 
                        --  AND a.prescription_id IN ('00150002356958','00150002356925')
                        order by a.prescription_id """
    source = pd.read_sql_query(query_source, conn_ehr)    

    return source

def fetch_target_data(source, conn_staging_sqlserver):
    """ ambil data dari tabel target, yaitu TransPatientDrugDispenseHeader """

    prescription_id = tuple(source["PrescriptionID"].unique())
    order_id = tuple(source["OrderID"].unique())
    patient_id = tuple(source["PatientID"].unique())
    admission_id = tuple(source["AdmissionID"].unique())

    # bikin function remove comma jika ada variable yang isinya cuma 1, variable ini akan di pakai IN CLAUSE di query target
    def remove_comma(x):
        if len(x) == 1:
            return str(x).replace(',','')
        else:
            return x
    
    prescription_id = remove_comma(prescription_id)
    order_id = remove_comma(order_id)
    patient_id = remove_comma(patient_id)
    admission_id = remove_comma(admission_id)

    query_target = f"""SELECT 
                        TRIM(PrescriptionID) AS PrescriptionID,
                        OrderID, 
                        PatientID, 
                        AdmissionID,  
                        MedicalNo,
                        PayplanID,
                        DepoOrgID,
                        OrderDate, 
                        Status,
                        CreatedUserID,
                        BillingID,
                        BillingPaid,
                        BillingAmount 
                      FROM staging_rscm.TransPatientDrugDispenseHeader 
                      WHERE PrescriptionID IN {prescription_id} AND OrderID IN {order_id} 
                      AND PatientID IN {patient_id} AND AdmissionID IN {admission_id}
                      ORDER BY PrescriptionID"""
    
    target = pd.read_sql_query(query_target,conn_staging_sqlserver)
    
    return target
   
def detect_changes(source, target):
    """ deteksi perubahan antara dataframe `source` dan `target` """
    
    # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

    # ambil data yang update
    modified = changes[changes[['PrescriptionID','OrderID','PatientID','AdmissionID']].apply(tuple,1).isin(target[['PrescriptionID','OrderID','PatientID','AdmissionID']].apply(tuple,1))]

    # ambil data yang baru
    inserted = changes[~changes[['PrescriptionID','OrderID','PatientID','AdmissionID']].apply(tuple,1).isin(target[['PrescriptionID','OrderID','PatientID','AdmissionID']].apply(tuple,1))]

    return modified, inserted

def updated_data(df, table_name, key_1,key_2,key_3,key_4, conn_staging_sqlserver):
    """ update data di tabel target """
    if not df.empty:
        a = [f't.{col} = s.{col}' for col in df.columns if col != key_1 and col != key_2 and col != key_3 and col != key_4]
        temp_table = f'{table_name}_temporary_table'
        
        # Upload the temp table to SQL Server
        df.to_sql(temp_table, schema='staging_rscm', con=conn_staging_sqlserver, if_exists = 'replace', index = False)
        update_stmt = (
            f'UPDATE t SET ' + ", ".join(a) +
            f', t.UpdateDateStaging = CONVERT(DATETIME2(0), GETDATE(),120)'
            f' FROM staging_rscm.{table_name} t '
            f'INNER JOIN (SELECT * FROM staging_rscm.{temp_table}) AS s ON t.{key_1} = s.{key_1} AND t.{key_2} = s.{key_2} AND t.{key_3} = s.{key_3} AND t.{key_4} = s.{key_4}'
            f' WHERE t.{key_1} = s.{key_1} AND t.{key_2} = s.{key_2} AND t.{key_3} = s.{key_3} AND t.{key_4} = s.{key_4} ;'
        )
        delete_stmt = f'DROP TABLE staging_rscm.{temp_table};'
        print(update_stmt)
        
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
            inserted.to_sql('TransPatientDrugDispenseHeader', schema='staging_rscm', con=conn_staging_sqlserver, if_exists='append', index=False)
            print('Data Success Inserted')
    else:
        print('Tidak ada data yang baru')

def main():
    """ Fungsi utama untuk menjalankan semua proses"""

    conn_ehr, conn_staging_sqlserver,conn_ehr_live = get_connections()

    try:
        # Ambil data dari source
        source = get_source_data(conn_ehr)
        print("Source Data:")
        print(source)

        # Ambil data dari target 
        target = fetch_target_data(source, conn_staging_sqlserver)
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
        updated_data(modified, 'TransPatientDrugDispenseHeader', 'PrescriptionID','OrderID','PatientID','AdmissionID', conn_staging_sqlserver)
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