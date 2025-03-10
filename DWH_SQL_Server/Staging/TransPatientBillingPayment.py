import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pandas as pd
import pyodbc
import datetime as dt
date = dt.datetime.today()
import time
import sys

sys.stdout = open("C:/TestPython/DWH_SQL_Server/Staging/logs/LogTransPatientBillingPayment.txt","w")

t0 = time.time()

conn_mpi = db_connection.create_connection(db_connection.mpi)
conn_his = db_connection.create_connection(db_connection.replika_his)
conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)

source = pd.read_sql_query(""" 
                            SELECT 
                                a.billing_trnsct_id BillingTransactID,
                                a.payor_nm CompanyName, 
                                b.patient_id PatientID,
                                e.admission_dttm AdmissionDate,
                                a.created_dttm TransactionDate,
                                CASE
                                    WHEN d.invoice_id IS NULL THEN 
                                        CASE
                                            WHEN c.invoice_id IS NULL THEN 'Belum ada invoice'
                                            ELSE 'Sudah Ditagihkan'
                                        END
                                    ELSE 'Sudah Dibayarkan'
                                END AS Status
                            FROM xocp_his_billing_trnsct a 
                            LEFT JOIN xocp_his_billing b on a.billing_id = b.billing_id
                            LEFT JOIN xocp_his_invoice_nonbill_cash_item c on a.billing_trnsct_id = c.billing_trnsct_id
                            LEFT JOIN xocp_his_invoice_payment_item_trnsct d on a.billing_trnsct_id = d.billing_trnsct_id
                            LEFT JOIN xocp_his_patient_admission e on b.patient_id = e.patient_id and b.admission_id = e.admission_id
                            WHERE a.payplan_id = '17'
                            -- AND a.created_dttm >= '2024-02-01 00:00:00' AND a.created_dttm <= '2024-02-31 23:59:59'
                            AND (a.created_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 15 DAY), "%%Y-%%m-%%d 00:00:00") 
                            AND a.created_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 00:00:00"))
                            -- AND a.billing_trnsct_id IN (712570, 712695)
                    """, conn_his)
print(source)
source = source.drop_duplicates(subset='BillingTransactID')

PatientID = tuple(source["PatientID"].unique())

query_patient = f"SELECT patient_id AS PatientID, person_id AS PersonID, mrn as MedicalNo FROM patients WHERE patient_id IN {PatientID}"
patient = pd.read_sql_query(query_patient, conn_mpi)

PersonID = tuple(patient["PersonID"].unique())

query_person = f"SELECT person_id as PersonID, person_nm as PatientName FROM persons WHERE person_id IN {PersonID}"
person = pd.read_sql_query(query_person, conn_mpi)

source = source.merge(patient,how='left', on='PatientID').merge(person,how='left',on='PersonID')

new_order_columns = ['BillingTransactID','CompanyName','PatientName','MedicalNo','AdmissionDate','TransactionDate','Status']
source = source.reindex(columns = new_order_columns)

print(source.dtypes)

if source.empty:
    print('tidak ada data dari source')
else:
    BillingTransactID = tuple(source["BillingTransactID"].unique())

     # bikin function remove comma jika ada variable yang isinya cuma 1, variable ini akan di pakai IN CLAUSE di query target
    def remove_comma(x):
        if len(x) == 1:
            return str(x).replace(',','')
        else:
            return x
    BillingTransactID = remove_comma(BillingTransactID)

    # query buat narik data dari target lalu filter berdasarkan primary key
    query = f'SELECT BillingTransactID, CompanyName, PatientName, MedicalNo, AdmissionDate, TransactionDate,Status FROM staging_rscm.TransPatientBillingPayment WHERE BillingTransactID IN {BillingTransactID}'
    target = pd.read_sql_query(query, conn_staging_sqlserver)
    print(target.dtypes)
    # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

    # ambil data yang update
    modified = changes[changes[['BillingTransactID']].apply(tuple,1).isin(target[['BillingTransactID']].apply(tuple,1))]
    total_row_upd = len(modified)
    text_upd = f'total row update : {total_row_upd}'
    print(text_upd)
    print(modified)

    #ambil data yang baru
    inserted = changes[~changes[['BillingTransactID']].apply(tuple,1).isin(target[['BillingTransactID']].apply(tuple,1))]
    total_row_ins = len(inserted)
    text_ins = f'total row inserted : {total_row_ins}'
    print(text_ins)
    print(inserted)

    if modified.empty:
        if inserted.empty :
            print('tidak ada data yang baru dan berubah')
        else:
            inserted.to_sql('TransPatientBillingPayment', schema='staging_rscm' ,con=conn_staging_sqlserver, if_exists = 'append', index=False)
            print('success insert without update')
    else:
        # buat fungsi update data
        def updated_data(df, table_name, key_1):
            a = []
            table = table_name
            pk_1 = key_1
            temp_table = f'{table}_temporary_table'
            for col in df.columns:
                if col == pk_1 :
                    continue
                a.append(f't.{col} = s.{col}')
            df.to_sql(temp_table, schema='staging_rscm', con=conn_staging_sqlserver, if_exists = 'replace', index = False)
            update_stmt_1 = f'UPDATE t '
            update_stmt_2 = f'SET '
            update_stmt_3 = ", ".join(a)
            update_stmt_8 = f' , t.UpdateDateStaging = CONVERT(DATETIME2(0), GETDATE(),120)'
            update_stmt_4 = f' FROM staging_rscm.{table} t '
            update_stmt_5 = f'INNER JOIN (SELECT * FROM staging_rscm.{temp_table}) AS s ON t.{pk_1} = s.{pk_1}  '
            update_stmt_6 = f' WHERE t.{pk_1} = s.{pk_1} '
            update_stmt_7 = update_stmt_1 +  update_stmt_2 + update_stmt_3 + update_stmt_8 + update_stmt_4 + update_stmt_5 + update_stmt_6 +";"
            delete_stmt_1 = f'DROP TABLE staging_rscm.{temp_table} '
            print(update_stmt_7)
            conn_staging_sqlserver.execute(update_stmt_7)
            conn_staging_sqlserver.execute(delete_stmt_1)

        try:
            #update data
            updated_data(modified, 'TransPatientBillingPayment', 'BillingTransactID')

            #insert data
            inserted.to_sql('TransPatientBillingPayment', schema='staging_rscm', con=conn_staging_sqlserver, if_exists = 'append', index=False)

            print('all success updated and inserted')
        except Exception as e:
            print(e)

#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)
print('\n')

text=f'scheduler tanggal : {date}'
print(text)

db_connection.close_connection(conn_his)
db_connection.close_connection(conn_staging_sqlserver)
sys.stdout.close()


