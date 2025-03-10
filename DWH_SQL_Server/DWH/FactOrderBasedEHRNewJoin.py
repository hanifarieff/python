import sys
import psutil
import os
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pyodbc
import pandas as pd
import numpy as np
import time
import datetime as dt
date = dt.datetime.today()
from datetime import datetime, timedelta

sys.stdout = open("C:/TestPython/DWH_SQL_Server/DWH/logs/LogFactOrderBasedEHRNewJoin.txt","w")
t0 = time.time()

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
    """ Membuat koneksi ke database"""
    conn_ehr_live = db_connection.create_connection(db_connection.ehr_live)
    conn_ehr = db_connection.create_connection(db_connection.replika_ehr)
    conn_dwh_sqlserver = db_connection.create_connection(db_connection.dwh_sqlserver)
    return conn_ehr_live, conn_ehr, conn_dwh_sqlserver

def get_source_data(conn_ehr, conn_dwh_sqlserver):
    """ Extract data database sumber (EHR) """

    # buat variabel start_date dan end_date dengan mengambil 1 hari ke belakang untuk dimasukkan di query
    start_date = (date - timedelta(days=2)).strftime('%Y-%m-%d')
    end_date = (date - timedelta(days=1)).strftime('%Y-%m-%d')
    # start_date = f'2024-12-23'
    # end_date = f'2024-12-23'

    # query untuk menarik data dari patient_order lalu masukkan variabel date_range di dalam WHERE
    query_source = f"""                            
                    SELECT 
                        a.order_id as OrderID,
                        a.patient_id PatientID,
                        a.admission_id AdmissionID,
                        a.ordered_dttm OrderDate,
                        '-' AS MonthValue,
                        CASE	
                            WHEN d.obj_year IS NULL THEN '-'
                            ELSE d.obj_year
                        END AS YearValue,
                        a.nullified_dttm as NullifiedDate,
                        e.admission_dttm AdmissionDate,
                        e.payplan_attr2 SEPNo,
                        a.obj_id as ObjID,
                        a.hcp_id as OrgID,
                        d.obj_nm ObjName,
                        j.obj_nm as ObjRoleName,
                        CASE
                                WHEN c.role_no IS NULL THEN '-'
                                ELSE c.role_no
                        END AS RoleNo,
                        b.tariff Tarif,
                        c.tariff - c.disc_tariff AS JasaMedis,
                        SUM(k.tariff) as JasaSarana,
                        a.payplan_id PayplanID,
                        CASE
                                WHEN c.obj_id LIKE '%%.%%' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(c.obj_id,'.',2),'.',-1)
                                ELSE '-'
                        END AS EmployeeID,
                        CASE
                                WHEN a.status_cd IN ('new','normal') THEN 'normal'
                                WHEN a.status_cd IN ('nullified','cancelled') THEN 'nullified'
                                ELSE a.status_cd
                        END AS Status,
                        'P' AS Type,
                        a.billing_paid as BillingPaid,
                        CASE 
                            WHEN d.tariff_class IS NULL THEN '-'
                            ELSE d.tariff_class
                        END AS TarifClass,
                        CASE
                            WHEN d.anesthesia_cd IS NULL THEN '-'
                            ELSE d.anesthesia_cd
                        END AS AnesthesiaCode
                        FROM
                        xocp_ehr_patient_order AS a
                        LEFT JOIN xocp_ehr_payplan_obj AS b ON a.obj_id = b.obj_id AND a.payplan_id = b.payplan_id
                        LEFT JOIN xocp_ehr_patient_order_role AS c ON a. order_id = c.order_id AND a.payplan_id = c.payplan_id
                        LEFT JOIN xocp_ehr_obj AS d ON a.obj_id = d.obj_id
                        LEFT JOIN xocp_ehr_patient_admission AS e ON a.patient_id = e.patient_id AND a.admission_id = e.admission_id
                        LEFT JOIN xocp_orgs as i on a.hcp_id = i.org_id
                        LEFT JOIN xocp_ehr_obj j on c.default_obj_id = j.obj_id 
                        LEFT JOIN xocp_ehr_patient_order_acctobj k on a.order_id = k.order_id 
                        WHERE
                        -- a.created_dttm >= '2025-01-01 00:00:00' AND a.created_dttm <= '2025-01-05 23:59:59' AND
                        a.order_id = '00210007706311' AND
                        j.obj_nm NOT LIKE '%%JPND%%' 
                        AND k.obj_id LIKE 'SRN%%' 
                        -- AND ((a.created_dttm >= '2024-12-10 00:00:00' AND a.created_dttm <= '2024-12-10 23:59:59')
                        -- OR (c.updated_dttm >= '2024-12-10 00:00:00' and c.updated_dttm <= '2024-12-10 23:59:59'))
                        -- a.ordered_dttm >=  00:00:00' AND a.ordered_dttm <=  23:59:59'
                        --                             (a.created_dttm >= '{start_date} 00:00:00' AND a.created_dttm <= '{end_date} 23:59:59') 
                        --                             OR (c.updated_dttm >= '{start_date} 00:00:00' and c.updated_dttm <= '{end_date} 23:59:59')

                        -- AND a.status_cd NOT IN ('nullified','cancelled')	
                        -- and a.order_id IN ('00210005401753','00210005401786')	
                        GROUP BY 
                        a.order_id,
                        a.patient_id,
                        a.admission_id,
                        a.ordered_dttm,
                        a.nullified_dttm,
                        e.admission_dttm,
                        c.role_no,
                        e.payplan_attr2,
                        a.obj_id,
                        a.hcp_id,
                        d.obj_nm,
                        j.obj_nm,
                        b.tariff,
                        a.payplan_id,
                        a.status_cd
                        ORDER BY a.order_id        
                    """
    source = pd.read_sql_query(query_source,conn_ehr)
    print(source)
    source = source[source['EmployeeID'] != '-']
    print(source)

    # define conditions
    conditions = [
        source['ObjName'].str.contains('Konsul', case=False, na=False),
        source['ObjName'].str.contains('Visit', case=False, na=False),
        source['ObjName'].str.contains('Lab', case=False, na=False),
        source['ObjName'].str.contains('Radiologi', case=False, na=False)
    ]
    choices =  ['jalan','inap','penunjang','penunjang']

    # bikin kolom baru CategoryName, bikin berdasarkan kondisi diatas
    source['CategoryName'] = np.select(conditions,choices,default='tindakan')
    print(source)
    
    # # ubah tipe data pada kolom2 tertentu menyesuaikan dengan yang ada di tabel
    source['EmployeeID'] = source['EmployeeID'].astype('int64')
    # source['Tarif'] = source['Tarif'].astype('float64')
    # source['JasaMedis'] = source['JasaMedis'].astype('float64')
    # source['JasaSarana'] = source['JasaSarana'].astype('float64')
    source['NullifiedDate'] = source['NullifiedDate'].astype('datetime64[ns]')
    source['VerifiedDate'] = source['VerifiedDate'].astype('datetime64[ns]')

    # cek kolom ObjName yang mengandung kata2 berikut dan CategoryNamenya tindakan, diubah CategoryName nya jadi penunjang  
    for i in source.index:
        cat = source.loc[i, 'CategoryName']
        obj = source.loc[i, 'ObjName'].lower()
        if cat == 'tindakan':
            if ('usg' in obj) | (obj.startswith('ct ')) | (obj.startswith('mri ')) | ('sgpt' in obj) \
                | ('sgot' in obj) | ('osmolaritas plasma' in obj) | ('ferritin' in obj) | ('ultrasound' in obj) \
                | ('ekg' in obj) | ('eeg' in obj) | ('tinja' in obj) | ('trigli' in obj) \
                | ('puasa' in obj) | (obj.startswith('anti ')) | ('gliko' in obj) | ('hb' in obj) | ('sputum' in obj):
                source.loc[i, 'CategoryName'] = 'penunjang'
    
    # bikin query untuk ambil kolom ObjectGroupingName
    query_object_grouping = f""" SELECT ObjectID as ObjID ,ObjectGroupingName 
                                FROM [dwhrscm_talend].[DimObjectGroupingDetail] dt
                                LEFT join dwhrscm_talend.DimObjectGroupingMaster dm on dt.ObjectGroupingID = dm.ObjectGroupingD
                                WHERE dt.SCDActive = 1 
                            """
    object_grouping = pd.read_sql_query(query_object_grouping, conn_dwh_sqlserver)
    source = source.merge(object_grouping,how='left',on='ObjID')
    
    source['ObjectGroupingName'].replace({np.nan: None},inplace=True)

    patient_id = tuple(source["PatientID"].unique())
    doctor_id = tuple(source["EmployeeID"].unique())

    def remove_comma(x):
        if len(x) == 1:
            return str(x).replace(',','')
        else:
            return x

    patient_id = remove_comma(patient_id)
    doctor_id = remove_comma(doctor_id)

    query_mpi = f""" SELECT 
                        PatientID,
                        PatientName,
                        MedicalNo
                    FROM dwhrscm_talend.DimPatientMPI
                    WHERE PatientID in {patient_id} AND ScdActive='1' 
                """
    mpi = pd.read_sql_query(query_mpi,conn_dwh_sqlserver)

    query_doctor = f""" 
                    SELECT
                        a.EmployeeID,
                        a.EmployeeName as DoctorName,
                        b.ChildOrganizationName as KSM,
                        a.EmployeeNo as NIP,
                        a.NIK
                    FROM dwhrscm_talend.DimEmployee a
                    LEFT JOIN dwhrscm_talend.DimOrganization b on a.UnitOrgID = b.ChildOrganizationID and b.SCDActive = '1'
                    WHERE a.EmployeeID IN {doctor_id} AND  a.SCDActive = '1'
                    """
    doctor = pd.read_sql_query(query_doctor,conn_dwh_sqlserver)
    source=source.merge(mpi,on='PatientID',how='left').merge(doctor, on='EmployeeID',how='left')
    
    new_order_columns = ['OrderID','PatientID','AdmissionID','EmployeeID','DoctorName','KSM','NIP','NIK','PatientName','MedicalNo'
                         ,'SEPNo','OrgName','AdmissionDate','OrderDate','NullifiedDate','VerifiedDate','ObjID','ObjName','ObjRoleName'
                         ,'RoleNo','IDItem','MonthValue','YearValue','Tarif','JasaMedis','JasaSarana','JasaRemun','PayplanName'
                         ,'CategoryName','PayplanKemkes','Type','Status','BillingPaid','ObjectGroupingName','OrgID','TarifClass'
                         ,'AnesthesiaCode']
    source = source.reindex(columns=new_order_columns)
    print(source)
    # source = source.merge(object_grouping,how='left',on='ObjID')
    
    # source['ObjectGroupingName'].replace({np.nan: None},inplace=True)

    return source

def get_target_data(source, conn_dwh_sqlserver):
    """ ambil data dari tabel target, yaitu FactOrderBasedNew """

    # masukkan data tarikan query ke tabel temporary, 
    source.to_sql('FactOrderBasedNewTemporary', schema='dwhrscm_talend',con=conn_dwh_sqlserver,if_exists='replace', index=False)
    query_filter = f"""SELECT OrderID,PatientID,AdmissionID,EmployeeID,DoctorName,KSM,NIP,NIK,PatientName,MedicalNo,SEPNo,OrgName,AdmissionDate,
                        OrderDate,NullifiedDate,VerifiedDate,ObjID,ObjName,ObjRoleName,RoleNo,IDItem,MonthValue,YearValue,Tarif,JasaMedis,JasaSarana,
                        JasaRemun,PayplanName,CategoryName,PayplanKemkes,Type,Status,BillingPaid,ObjectGroupingName,OrgID,TarifClass,AnesthesiaCode
                        FROM dwhrscm_talend.FactOrderBasedNew where OrderID IN (SELECT OrderID FROM dwhrscm_talend.FactOrderBasedNewTemporary) 
                        ORDER BY OrderID"""
    target = pd.read_sql_query(query_filter,conn_dwh_sqlserver)

    target['NullifiedDate'] = target['NullifiedDate'].astype('datetime64[ns]')
    target['VerifiedDate'] = target['VerifiedDate'].astype('datetime64[ns]')

    query_drop_table = f"DROP TABLE dwhrscm_talend.FactOrderBasedNewTemporary"
    conn_dwh_sqlserver.execute(query_drop_table)
    target['ObjectGroupingName'].replace({np.nan: None},inplace=True)
    print('ini target')
    print(target.dtypes)
    return target

def detect_changes(source,target) :
    """ deteksi perubahan antara dataframe `source` dan `target` """

    # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

    # ambil data yang update
    modified = changes[changes[['OrderID','RoleNo']].apply(tuple,1).isin(target[['OrderID','RoleNo']].apply(tuple,1))]

    # ambil data yang baru
    inserted = changes[~changes[['OrderID','RoleNo']].apply(tuple,1).isin(target[['OrderID','RoleNo']].apply(tuple,1))]

    return modified, inserted

def updated_data(df, table_name, key_1, key_2,conn_dwh_sqlserver):
    """ update data di tabel target yaitu TransPrescriptionResponsTime"""
    if not df.empty:
        a = [f't.{col} = s.{col}' for col in df.columns if col != key_1 and col != key_2 ]
        temp_table = f'{table_name}_temporary_table'
        
        # Upload the temp table to SQL Server
        df.to_sql(temp_table, schema='dwhrscm_talend', con=conn_dwh_sqlserver, if_exists = 'replace', index = False)
        update_stmt = (
            f'UPDATE t SET ' + ", ".join(a) +
            f', t.UpdateDateDWH = CONVERT(DATETIME2(0), GETDATE(),120)'
            f' FROM dwhrscm_talend.{table_name} t '
            f'INNER JOIN (SELECT * FROM dwhrscm_talend.{temp_table}) AS s ON t.{key_1} = s.{key_1} AND t.{key_2} = s.{key_2}  '
            f'WHERE t.{key_1} = s.{key_1} AND t.{key_2} = s.{key_2} ;'
        )
        delete_stmt = f'DROP TABLE dwhrscm_talend.{temp_table};'
        
        with conn_dwh_sqlserver.begin() as transaction:
            # Execute update and delete temp table
            conn_dwh_sqlserver.execute(update_stmt)
            conn_dwh_sqlserver.execute(delete_stmt)
            print('\nData Success Updated')
    else:
        print('\nTidak ada data yang berubah')   

def inserted_data(inserted, conn_dwh_sqlserver) :
    if not inserted.empty:
        with conn_dwh_sqlserver.begin() as transaction:
            inserted.to_sql('FactOrderBasedNew', schema='dwhrscm_talend', con=conn_dwh_sqlserver, if_exists='append', index=False)
            print('Data Success Inserted')
    else:
        print('Tidak ada data yang baru')

def main():
    """ Fungsi utama untuk menjalankan semua proses"""

    conn_ehr_live ,conn_ehr, conn_dwh_sqlserver = get_connections()

    try:
         # Ambil data dari source
        source = get_source_data(conn_ehr,conn_dwh_sqlserver)
        print("Source Data:")
        # print(source)
        print(source.dtypes)

        # Ambil data dari target 
        target = get_target_data(source, conn_dwh_sqlserver)
        print("Target Data:")
        print(target)
        print(target.dtypes)

        # Deteksi perubahan (buat dapetin modified dan inserted)
        modified, inserted = detect_changes(source, target)
        print("Changes Detected:")
        print("Modified Data:")
        print(modified)
        print("Inserted Data:")
        print(inserted.loc[:,['OrderID','EmployeeID','OrgName','CategoryName','ObjectGroupingName']])

        inserted.to_excel('Data Baru Masuk.xlsx',sheet_name ='Sheet1',index=False)

        # update data
        updated_data(modified, 'FactOrderBasedNew', 'OrderID','RoleNo', conn_dwh_sqlserver)

        # insert data
        inserted_data(inserted, conn_dwh_sqlserver)
    finally:
        db_connection.close_connection(conn_ehr_live)
        db_connection.close_connection(conn_ehr)
        db_connection.close_connection(conn_dwh_sqlserver)

# Run the main process
if __name__ == "__main__":
    main()

#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)


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

sys.stdout.close()