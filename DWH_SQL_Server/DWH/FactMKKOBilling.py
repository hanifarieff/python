import sys
sys.path.insert(1,'C://TestPython//connection')
# sys.path.insert(1,'E://WORK_RSCM//Python//Connection')
import db_connection
import pandas as pd
import numpy as np
import time
import datetime as dt
date = dt.datetime.today()
from datetime import datetime, timedelta

# bikin log ke file
sys.stdout = open("C:/TestPython/DWH_SQL_Server/DWH/logs/LogFactMKKOBiling.txt","w")
t0 = time.time()

# bikin koneksi ke db
conn_dwh_sqlserver = db_connection.create_connection(db_connection.dwh_sqlserver)
conn_ehr_live = db_connection.create_connection(db_connection.ehr_live)
conn_his_live = db_connection.create_connection(db_connection.his_live)

# bikin variabel start_date dan end_date untuk memasukkan range tanggal awal dan akhir untuk diletakkan di WHERE query source 1,2,3
# Ambil tanggal dan jam hari ini
# today = datetime.now() - timedelta(days=1)
# start_date = today.strftime("'%d-%m-%Y 00:00:00'")
# end_date = today.strftime("'%d-%m-%Y 23:59:59'")

# start_date = f"DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), '%%Y-%%m-%%d 00:00:00')"
# end_date = f"DATE_FORMAT(NOW(), '%%Y-%%m-%%d 23:59:59')"
start_date = f"'2024-08-01 00:00:00'"
end_date = f"'2024-08-01 23:59:59'"
print(start_date)
print(end_date)

# outpatient umum reguler
query_1 = f""" SELECT 
                                    a.trnsct_id as TransactionID, 
                                    a.billing_id as BillingID, 
                                    a.trnsct_amount as Amount, 
                                    a.trnsct_dttm as TransactionDate, 
                                    b.payplan_id as PayplanID,
                                    'outpatient' as PatientType,
                                    'umum' as PayplanGroup,
                                    'reguler' as PayplanType,
                                    c.payplan_attr2 as PayplanNo,
                                    'pelayanan' as RevenueType,
                                    b.patient_id as PatientID,
                                    b.admission_id as AdmissionID,
                                    b.status_cd as Status
                                FROM xocp_ehr_billing_trnsct a
                                LEFT JOIN xocp_ehr_billing b ON b.billing_id = a.billing_id
                                LEFT JOIN xocp_ehr_patient_admission c ON c.patient_id = b.patient_id AND c.admission_id = b.admission_id
                                WHERE a.trnsct_dttm >= {start_date} AND a.trnsct_dttm <= {end_date}
                                AND a.status_cd = 'normal'
                                AND b.status_cd = 'final'
                                AND b.payplan_id = '8'
                                AND c.stay_ind = 'n'
                                
                            """
source1 = pd.read_sql_query(query_1,conn_ehr_live)


# outpatient perusahaan reguler
query_2 = f""" SELECT 
                                    a.trnsct_id as TransactionID, 
                                    a.billing_id as BillingID, 
                                    a.trnsct_amount as Amount, 
                                    a.trnsct_dttm as TransactionDate, 
                                    b.payplan_id as PayplanID,
                                    'outpatient' AS PatientType,
                                    'perusahaan' as PayplanGroup,
                                    'reguler' as PayplanType,
                                    c.payplan_attr2 as PayplanNo,
                                    'pelayanan' as RevenueType,
                                    b.patient_id as PatientID,
                                    b.admission_id as AdmissionID,
                                    b.status_cd as Status
                                FROM xocp_ehr_billing_trnsctpt a
                                LEFT JOIN xocp_ehr_billing b ON b.billing_id = a.billing_id
                                LEFT JOIN xocp_ehr_patient_admission c ON c.patient_id = b.patient_id AND c.admission_id = b.admission_id
                                WHERE a.trnsct_dttm >= {start_date} AND a.trnsct_dttm <= {end_date}
                                AND a.status_cd = 'normal'
                                AND b.status_cd = 'final'
                                AND b.payplan_id NOT IN ('8','10', '11', '35', '71')
                                AND c.stay_ind = 'n' 

                               
                            """
source2 = pd.read_sql_query(query_2,conn_ehr_live)

# outpatient asuransi reguler 
query_3 = f""" 
                            SELECT 
                                a.trnsct_id as TransactionID, 
                                a.billing_id as BillingID, 
                                a.trnsct_amount as Amount, 
                                a.trnsct_dttm as TransactionDate, 
                                b.payplan_id as PayplanID,
                                'outpatient' as PatientType,
                                'asuransi' as PayplanGroup,
                                'reguler' as PayplanType,
                                c.payplan_attr2 as PayplanNo,
                                'pelayanan' as RevenueType,
                                b.patient_id as PatientID,
                                b.admission_id as AdmissionID,
                                b.status_cd as Status
                            FROM xocp_ehr_billing_trnsctpt a
                            LEFT JOIN xocp_ehr_billing b ON b.billing_id = a.billing_id
                            LEFT JOIN xocp_ehr_patient_admission c ON c.patient_id = b.patient_id AND c.admission_id = b.admission_id
                            WHERE a.trnsct_dttm >= {start_date} AND a.trnsct_dttm <= {end_date}
                            AND a.status_cd = 'normal'
                            AND b.status_cd = 'final'
                            AND b.payplan_id IN ('10', '11', '35')
                            AND c.stay_ind = 'n'
                          """
source3 = pd.read_sql_query(query_3,conn_ehr_live)

# outpatient JKN reguler
query_4 = f""" SELECT 
                    a.trnsct_id as TransactionID, 
                    a.billing_id as BillingID, 
                    a.trnsct_amount as Amount, 
                    a.trnsct_dttm as TransactionDate, 
                    b.payplan_id as PayplanID,
                    'outpatient' as PatientType,
                    'jkn' as PayplanGroup,
                    'reguler' as PayplanType,
                    c.payplan_attr2 as PayplanNo,
                    'pelayanan' as RevenueType,
                    b.patient_id as PatientID,
                    b.admission_id as AdmissionID,
                    b.status_cd as Status
                FROM xocp_ehr_billing_trnsctpt a
                LEFT JOIN xocp_ehr_billing b ON b.billing_id = a.billing_id
                LEFT JOIN xocp_ehr_patient_admission c ON c.patient_id = b.patient_id AND c.admission_id = b.admission_id
                WHERE a.trnsct_dttm >= {start_date} AND a.trnsct_dttm <= {end_date}
                AND a.status_cd = 'normal'
                AND b.status_cd = 'final'
                AND b.payplan_id = '71'
                AND c.stay_ind = 'n' """
source4 = pd.read_sql_query(query_4,conn_ehr_live)

# inpatient umum reguler
query_5 = f""" 
                            SELECT 
                                a.trnsct_id as TransactionID, 
                                a.billing_id as BillingID, 
                                a.trnsct_amount as Amount, 
                                a.trnsct_dttm as TransactionDate, 
                                b.payplan_id as PayplanID, 
                                'inpatient' as PatientType,
                                'umum' as PayplanGroup,
                                'reguler' as PayplanType,
                                c.payplan_attr2 as PayplanNo,
                                'pelayanan' as RevenueType,
                                b.patient_id as PatientID,
                                b.admission_id as AdmissionID,
                                b.status_cd as Status
                            FROM xocp_ehr_billing_trnsct a
                            LEFT JOIN xocp_ehr_billing b ON b.billing_id = a.billing_id
                            LEFT JOIN xocp_ehr_patient_admission c ON c.patient_id = b.patient_id AND c.admission_id = b.admission_id
                            WHERE a.trnsct_dttm >= {start_date} AND a.trnsct_dttm <= {end_date} 
                            AND a.status_cd = 'normal'
                            AND b.status_cd = 'final'
                            AND b.payplan_id = '8' 
                            AND c.stay_ind = 'y'
                            """
source5 = pd.read_sql_query(query_5,conn_ehr_live)

# inpatient perusahaan reguler
query_6 = f""" 
                            SELECT 
                                a.trnsct_id as TransactionID, 
                                a.billing_id as BillingID, 
                                a.trnsct_amount as Amount, 
                                a.trnsct_dttm as TransactionDate, 
                                b.payplan_id as PayplanID, 
                                'inpatient' as PatientType,
                                'perusahaan' as PayplanGroup,
                                'reguler' as PayplanType,
                                c.payplan_attr2 as PayplanNo,
                                'pelayanan' as RevenueType,
                                b.patient_id as PatientID,
                                b.admission_id as AdmissionID,
                                b.status_cd as Status
                            FROM xocp_ehr_billing_trnsctpt a
                            LEFT JOIN xocp_ehr_billing b ON b.billing_id = a.billing_id
                            LEFT JOIN xocp_ehr_patient_admission c ON c.patient_id = b.patient_id AND c.admission_id = b.admission_id
                            WHERE a.trnsct_dttm >= {start_date} AND a.trnsct_dttm <= {end_date} 
                            AND a.status_cd = 'normal'
                            AND b.status_cd = 'final'
                            AND b.payplan_id NOT IN ('8','10', '11', '35', '71')
                            AND c.stay_ind = 'y'
                            """
source6 = pd.read_sql_query(query_6,conn_ehr_live)

# inpatient asuransi reguler
query_7 = f""" 
                            SELECT 
                                a.trnsct_id as TransactionID, 
                                a.billing_id as BillingID, 
                                a.trnsct_amount as Amount, 
                                a.trnsct_dttm as TransactionDate, 
                                b.payplan_id as PayplanID, 
                                'inpatient' as PatientType,
                                'asuransi' as PayplanGroup,
                                'reguler' as PayplanType,
                                c.payplan_attr2 as PayplanNo,
                                'pelayanan' as RevenueType,
                                b.patient_id as PatientID,
                                b.admission_id as AdmissionID,
                                b.status_cd as Status
                            FROM xocp_ehr_billing_trnsctpt a
                            LEFT JOIN xocp_ehr_billing b ON b.billing_id = a.billing_id
                            LEFT JOIN xocp_ehr_patient_admission c ON c.patient_id = b.patient_id AND c.admission_id = b.admission_id
                            WHERE a.trnsct_dttm >= {start_date} AND a.trnsct_dttm <= {end_date}
                            AND a.status_cd = 'normal'
                            AND b.status_cd = 'final'
                            AND b.payplan_id IN ('10', '11', '35')
                            AND c.stay_ind = 'y'
                            """
source7 = pd.read_sql_query(query_7,conn_ehr_live)

# inpatient JKN Reguler
query_8 = f""" SELECT 
                    a.trnsct_id as TransactionID, 
                    a.billing_id as BillingID, 
                    a.trnsct_amount as Amount, 
                    a.trnsct_dttm as TransactionDate, 
                    b.payplan_id as PayplanID,
                    'inpatient' as PatientType,
                    'jkn' as PayplanGroup,
                    'reguler' as PayplanType,
                    c.payplan_attr2 as PayplanNo,
                    'pelayanan' as RevenueType,
                    b.patient_id as PatientID,
                    b.admission_id as AdmissionID,
                    b.status_cd as Status
                FROM xocp_ehr_billing_trnsctpt a
                LEFT JOIN xocp_ehr_billing b ON b.billing_id = a.billing_id
                LEFT JOIN xocp_ehr_patient_admission c ON c.patient_id = b.patient_id AND c.admission_id = b.admission_id
                WHERE a.trnsct_dttm >= {start_date} AND a.trnsct_dttm <= {end_date}
                AND a.status_cd = 'normal'
                AND b.status_cd = 'final'
                AND b.payplan_id = '71'
                AND c.stay_ind = 'y' """
source8 = pd.read_sql_query(query_8,conn_ehr_live)

### INI HIS EKSEKUTIF
# Outpatient asuransi eksekutif                          
query_9 = f""" 
                            SELECT 
                                a.billing_trnsct_id as TransactionID, 
                                b.billing_id AS BillingID, 
                                a.trnsct_amount as Amount, 
                                b.final_dttm as TransactionDate, 
                                a.payplan_id as PayplanID, 
                                d.admission_type as PatientType,
                                'asuransi' as PayplanGroup,
                                'eksekutif' as PayplanType,
                                d.payplan_claim_number as PayplanNo,
                                'pelayanan' as RevenueType,
                                b.patient_id as PatientID,
                                b.admission_id as AdmissionID,
                                b.status_cd as Status
                            FROM xocp_his_billing_trnsct a
                            LEFT JOIN xocp_his_billing b on b.billing_id = a.billing_id
                            LEFT JOIN xocp_his_company_payplan c ON c.payplan_id = a.payplan_id AND c.company_id = a.company_id
                            LEFT JOIN xocp_his_patient_admission d ON d.patient_id = b.patient_id AND d.admission_id = b.admission_id
                            WHERE a.payor_t IN ('payor1_pay','payor2_pay','payor3_pay') 
                            AND a.status_cd = 'normal'
                            AND b.final_dttm >= {start_date} AND b.final_dttm <= {end_date}
                            AND a.payplan_id = '17' AND a.company_id != '0'
                            AND b.status_cd = 'final'
                            AND d.admission_type = 'outpatient'
                            AND c.company_type = 'Asuransi'
                            """
source9 = pd.read_sql_query(query_9,conn_his_live)

# outpatient perusahaan eksekutif
query_10 = f""" 
                            SELECT 
                                a.billing_trnsct_id as TransactionID, 
                                b.billing_id as BillingID, 
                                a.trnsct_amount as Amount, 
                                b.final_dttm as TransactionDate, 
                                a.payplan_id as PayplanID, 
                                d.admission_type as PatientType,
                                'perusahaan' as PayplanGroup,
                                'eksekutif' as PayplanType,
                                d.payplan_claim_number as PayplanNo,
                                'pelayanan' as RevenueType,
                                b.patient_id as PatientID,
                                b.admission_id as AdmissionID,
                                b.status_cd as Status
                            FROM xocp_his_billing_trnsct a
                            LEFT JOIN xocp_his_billing b on b.billing_id = a.billing_id
                            LEFT JOIN xocp_his_company_payplan c ON c.payplan_id = a.payplan_id AND c.company_id = a.company_id
                            LEFT JOIN xocp_his_patient_admission d ON d.patient_id = b.patient_id AND d.admission_id = b.admission_id
                            WHERE a.payor_t IN ('payor1_pay','payor2_pay','payor3_pay') 
                            AND a.status_cd = 'normal'
                            AND b.final_dttm >= {start_date} AND b.final_dttm <= {end_date}
                            AND a.payplan_id = '17' AND a.company_id != '0'
                            AND b.status_cd = 'final'
                            AND d.admission_type = 'outpatient'
                            AND c.company_type != 'Asuransi'
                            """
source10 = pd.read_sql_query(query_10,conn_his_live)

# outpatient umum eksekutif
query_11 = f""" 
                            SELECT 
                                a.billing_trnsct_id AS TransactionID, 
                                b.billing_id as BillingID, 
                                a.trnsct_amount as Amount, 
                                b.final_dttm as TransactionDate, 
                                b.payplan_id as PayplanID,
                                c.admission_type as PatientType,
                                'umum' as PayplanGroup,
                                'eksekutif' as PayplanType,
                                c.payplan_claim_number as PayplanNo,
                                'pelayanan' as RevenueType,
                                b.patient_id as PatientID,
                                b.admission_id as AdmissionID,
                                b.status_cd as Status
                            FROM xocp_his_billing_trnsct a
                            LEFT JOIN xocp_his_billing b ON b.billing_id = a.billing_id
                            LEFT JOIN xocp_his_patient_admission c ON c.patient_id = b.patient_id AND c.admission_id = b.admission_id
                            WHERE a.payor_t = 'patient_pay' 
                            AND a.status_cd = 'normal'
                            AND b.final_dttm >= {start_date} AND b.final_dttm <= {end_date} 
                            AND b.payplan_id = '8' 
                            AND b.status_cd = 'final' 
                            AND c.admission_type = 'outpatient'
                            """
source11 = pd.read_sql_query(query_11,conn_his_live)

# inpatient asuransi eksekutif
query_12 = f""" 
                            SELECT 
                                a.billing_trnsct_id as TransactionID, 
                                b.billing_id as BillingID, 
                                a.trnsct_amount as Amount, 
                                b.final_dttm as TransactionDate, 
                                a.payplan_id as PayplanID, 
                                d.admission_type as PatientType,
                                'asuransi' as PayplanGroup,
                                'eksekutif' as PayplanType,
                                d.payplan_claim_number as PayplanNo,
                                'pelayanan' as RevenueType,
                                b.patient_id as PatientID,
                                b.admission_id as AdmissionID,
                                b.status_cd as Status
                            FROM xocp_his_billing_trnsct a
                            LEFT JOIN xocp_his_billing b on b.billing_id = a.billing_id
                            LEFT JOIN xocp_his_company_payplan c ON c.payplan_id = a.payplan_id AND c.company_id = a.company_id
                            LEFT JOIN xocp_his_patient_admission d ON d.patient_id = b.patient_id AND d.admission_id = b.admission_id
                            WHERE a.payor_t IN ('payor1_pay','payor2_pay','payor3_pay') 
                            AND a.status_cd = 'normal'
                            AND b.final_dttm >= {start_date} AND b.final_dttm <= {end_date}
                            AND a.payplan_id = '17' AND a.company_id != '0'
                            AND b.status_cd = 'final'
                            AND d.admission_type = 'inpatient'
                            AND c.company_type = 'Asuransi'
                            """
source12 = pd.read_sql_query(query_12,conn_his_live)

# inpatient perusahaan eksekutif
query_13 = f""" 
                            SELECT 
                                a.billing_trnsct_id as TransactionID, 
                                b.billing_id as BillingID, 
                                a.trnsct_amount as Amount, 
                                b.final_dttm as TransactionDate, 
                                a.payplan_id as PayplanID, 
                                d.admission_type as PatientType,
                                'perusahaan'as PayplanGroup,
                                'eksekutif' as PayplanType,
                                d.payplan_claim_number as PayplanNo,
                                'pelayanan' as RevenueType,
                                b.patient_id as PatientID,
                                b.admission_id as AdmissionID,
                                b.status_cd as Status
                            FROM xocp_his_billing_trnsct a
                            LEFT JOIN xocp_his_billing b on b.billing_id = a.billing_id
                            LEFT JOIN xocp_his_company_payplan c ON c.payplan_id = a.payplan_id AND c.company_id = a.company_id
                            LEFT JOIN xocp_his_patient_admission d ON d.patient_id = b.patient_id AND d.admission_id = b.admission_id
                            WHERE a.payor_t IN ('payor1_pay','payor2_pay','payor3_pay') 
                            AND a.status_cd = 'normal'
                            AND b.final_dttm >= {start_date} AND b.final_dttm <= {end_date}
                            AND a.payplan_id = '17' AND a.company_id != '0'
                            AND b.status_cd = 'final'
                            AND d.admission_type = 'inpatient'
                            AND c.company_type != 'Asuransi'
                            """
source13 = pd.read_sql_query(query_13,conn_his_live)

# inpatient umum eksekutif
query_14 = f""" 
                            SELECT 
                                a.billing_trnsct_id as TransactionID, 
                                b.billing_id as BillingID, 
                                a.trnsct_amount as Amount, 
                                b.final_dttm as TransactionDate, 
                                b.payplan_id as PayplanID, 
                                c.admission_type as PatientType,
                                'umum' as PayplanGroup,
                                'eksekutif' as PayplanType,
                                c.payplan_claim_number as PayplanNo,
                                'pelayanan' as RevenueType,
                                b.patient_id as PatientID,
                                b.admission_id as AdmissionID,
                                b.status_cd as Status
                            FROM xocp_his_billing_trnsct a
                            LEFT JOIN xocp_his_billing b ON b.billing_id = a.billing_id
                            LEFT JOIN xocp_his_patient_admission c ON c.patient_id = b.patient_id AND c.admission_id = b.admission_id
                            WHERE a.payor_t = 'patient_pay' 
                            AND a.status_cd = 'normal'
                            AND b.final_dttm >= {start_date} AND b.final_dttm <= {end_date}
                            AND b.payplan_id = '8' 
                            AND b.status_cd = 'final' 
                            AND c.admission_type = 'inpatient'
                            """
source14 = pd.read_sql_query(query_14,conn_his_live)

# pendapatan lainnya (penunjang)
query_15 = f""" 
                SELECT 
                    a.billing_trnsct_id as TransactionID, 
                    b.billing_id as BillingID, 
                    a.trnsct_amount as Amount, 
                    b.final_dttm as TransactionDate, 
                    a.payplan_id as PayplanID, 
                    d.admission_type as PatientType, 
                    'perusahaan' as PayplanGroup,
                    'reguler' as PayplanType,
                    d.payplan_claim_number as PayplanNo,
                    'penunjang' as RevenueType,
                    b.patient_id as PatientID,
                    b.admission_id as AdmissionID,
                    b.status_cd as Status
                FROM xocp_his_billing_trnsct a
                LEFT JOIN xocp_his_billing b on b.billing_id = a.billing_id
                LEFT JOIN xocp_his_company_payplan c ON c.payplan_id = a.payplan_id AND c.company_id = a.company_id
                LEFT JOIN xocp_his_patient_admission d ON d.patient_id = b.patient_id AND d.admission_id = b.admission_id
                WHERE a.status_cd = 'normal'
                AND b.final_dttm >= {start_date} AND b.final_dttm <= {end_date}
                AND a.payplan_id = '17' AND a.company_id = '0'
                AND b.status_cd = 'final' """
source15 = pd.read_sql_query(query_15,conn_his_live)

source_ehr = pd.concat([source1,source2,source3,source4,source5,source6,source7,source8], ignore_index=True)
source_his = pd.concat([source9,source10,source11,source12,source13,source14,source15], ignore_index=True)

source = pd.concat([source_ehr,source_his],ignore_index=True)
source = source.drop_duplicates(subset=['TransactionID','BillingID'])

source['Amount'] = source['Amount'].astype('float64')
source['PayplanID'] = source['PayplanID'].astype('int64')
source['TransactionID'] = source['TransactionID'].astype('str')
source['BillingID'] = source['BillingID'].astype('str')
# source=source[source['TransactionID']=='00210000004822']
print('ini source')
print(source.loc[:,['TransactionID','PayplanGroup','PayplanType']])

if source.empty:
    print('tidak ada data dari source')
else:
    transaction_id = tuple(source["TransactionID"].unique())
    billing_id = tuple(source["BillingID"].unique())

    if len(transaction_id) > 1:
        pass
    else:
        transaction_id = str(transaction_id).replace(',','')
    if len(billing_id) > 1:
        pass
    else:
        billing_id = str(billing_id).replace(',','')

    query = f'SELECT TRIM(TransactionID) as TransactionID,BillingID,Amount,TransactionDate,PayplanID,PatientType,PayplanGroup,PayplanType,PayplanNo,RevenueType,PatientID,AdmissionID,Status FROM dwhrscm_talend.FactMKKOBilling where TransactionID IN {transaction_id} AND BillingID IN {billing_id} order by TransactionID'
    target = pd.read_sql_query(query, conn_dwh_sqlserver)

    # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

    # ambil data yang update dari changes
    modified = changes[changes[['TransactionID','BillingID']].apply(tuple,1).isin(target[['TransactionID','BillingID']].apply(tuple,1))]
    total_row_upd = len(modified)
    text_upd = f'total row update : {total_row_upd}'
    print(text_upd)
    print(modified)

    # ambil data yang new dari changes
    inserted = changes[~changes[['TransactionID','BillingID']].apply(tuple,1).isin(target[['TransactionID','BillingID']].apply(tuple,1))]
    total_row_ins = len(inserted)
    text_ins = f'total row inserted : {total_row_ins}'
    print(text_ins)
    print(inserted)
   
    if modified.empty:
        # bikin tanggal sekarang buat kolom InsertedDateDWH
        today = dt.datetime.now()
        today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
        inserted['InsertedDateDWH'] = today_convert
        inserted.to_sql('FactMKKOBilling', schema='dwhrscm_talend', con = conn_dwh_sqlserver, if_exists = 'append', index=False)
        print('success insert all data without update')
    
    else:
        # buat fungsi untuk update data ke tabel target
        def updated_to_sql(df, table_name, key_1,key_2):
            list_col = []
            table=table_name
            pk_1 = key_1
            pk_2 = key_2
            temp_table = f'{table}_temporary_table'
            for col in df.columns:
                if col == pk_1 or col == pk_2:
                    continue
                list_col.append(f'r.{col} = t.{col}')
            df.to_sql(temp_table,schema = 'dwhrscm_talend',con=conn_dwh_sqlserver, if_exists='replace',index=False)
            update_stmt_1 = f'UPDATE r '
            update_stmt_2 = f'SET '
            update_stmt_3 = ", ".join(list_col)
            update_stmt_8 = f' , r.UpdateDateDWH = CONVERT(DATETIME2(0), GETDATE(),120)'
            update_stmt_4 = f' FROM dwhrscm_talend.{table} r '
            update_stmt_5 = f'INNER JOIN (SELECT * FROM dwhrscm_talend.{temp_table}) as t ON r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2}  '
            update_stmt_6 = f'WHERE r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} '
            update_stmt_7 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_8 + update_stmt_4 + update_stmt_5 + update_stmt_6 +";"
            delete_stmt_1 = f'DROP TABLE dwhrscm_talend.{temp_table}'
            print(update_stmt_7)
            conn_dwh_sqlserver.execute(update_stmt_7)
            conn_dwh_sqlserver.execute(delete_stmt_1)

        try:
            with conn_dwh_sqlserver.begin() as transaction:
                # update data
                updated_to_sql(modified, 'FactMKKOBilling', 'TransactionID','BillingID')

                # insert data baru
                today = dt.datetime.now()
                today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
                inserted['InsertedDateDWH'] = today_convert
                inserted.to_sql('FactMKKOBilling', schema='dwhrscm_talend', con=conn_dwh_sqlserver, if_exists ='append',index=False)
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