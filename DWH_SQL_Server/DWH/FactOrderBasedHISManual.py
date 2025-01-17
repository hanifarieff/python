import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pyodbc
import pandas as pd
import numpy as np
import time
import datetime as dt
date = dt.datetime.today()
import psutil
import os

import sys

sys.stdout = open("C:/TestPython/DWH_SQL_Server/DWH/logs/LogFactOrderBasedHISOld.txt","w")
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
    conn_his_live = db_connection.create_connection(db_connection.his_live)
    conn_his = db_connection.create_connection(db_connection.replika_his)
    conn_ehr = db_connection.create_connection(db_connection.replika_ehr)
    conn_dwh_sqlserver = db_connection.create_connection(db_connection.dwh_sqlserver)
    conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)

    return conn_his_live, conn_his,conn_ehr, conn_dwh_sqlserver,conn_staging_sqlserver

def get_source_data(conn_his_live,conn_ehr,conn_dwh_sqlserver):
    """ Menarik data dari database sumber HIS"""

    # query untuk menarik data dari his_patient_order lalu masukkan variabel date_range di dalam WHERE
    query_source = f"""
                        SELECT 
                            a.order_id as OrderID,
                            a.patient_id as PatientID,
                            a.admission_id as AdmissionID,
                            CASE
                                    WHEN b.employee_id IS NULL THEN 0
                                    ELSE b.employee_id
                            END AS EmployeeID,
                            UPPER(psn.person_nm) as PatientName,
                            adm.admission_dttm as AdmissionDate,
                            a.ordered_dttm as OrderDate,
                            a.nullified_dttm as NullifiedDate,
                            NULL as VerifiedDate,
                            z.patient_mrn_txt as MedicalNo,
                            '-' AS SEPNo,
                            h.org_nm as OrgName,
                            a.obj_id as ObjID,
                            c.obj_nm as ObjName,
                            CASE
                                WHEN obj.obj_nm IS NULL THEN '-'
                                ELSE obj.obj_nm
                            END as ObjRoleName,
                            CASE 
                                WHEN b.role_no IS NULL THEN '-'
                                ELSE b.role_no
                            END AS RoleNo,
                            NULL AS IDItem,
                            MONTH(a.ordered_dttm) MonthValue,
                            YEAR(a.ordered_dttm) YearValue, 
                            f.tariff as Tarif,
                            CASE
                                WHEN b.tariff - b.discount IS NULL THEN '0'
                                ELSE b.tariff - b.discount
                            END AS JasaMedis,
                            NULL as JasaSarana,
                            NULL as JasaRemun,
                            g.payplan_nm as PayplanName,
                            CASE 
                                WHEN c.obj_nm LIKE '%%Konsul%%' THEN 'jalan'
                                WHEN c.obj_nm LIKE '%%Visit%%' THEN 'visitasi'
                                WHEN h.org_nm LIKE '%%Lab%%'  THEN 'penunjang'
                                WHEN h.org_nm LIKE '%%Radiologi%%' THEN 'penunjang'
                                ELSE 'tindakan'
                            END AS CategoryName,
                            CASE
                                WHEN a.payplan_id = 8 THEN 'Eksekutif'
                                ELSE 'Eksekutif Jaminan'
                            END AS PayplanKemkes,
                            'K' AS Type,
                            a.status_cd as Status,
                            a.billing_paid as BillingPaid
                        FROM xocp_his_patient_order AS a
                        LEFT JOIN xocp_his_patient_order_role AS b ON a.order_id = b.order_id
                        LEFT JOIN xocp_obj AS c ON a.obj_id = c.obj_id
                        -- LEFT JOIN xocp_employee AS d ON b.employee_id = d.employee_id
                        -- LEFT JOIN xocp_persons AS e ON d.person_id = e.person_id
                        LEFT JOIN xocp_his_obj_payplan AS f ON a.payplan_id = f.payplan_id AND a.obj_id = f.obj_id
                        LEFT JOIN xocp_his_payplan AS g ON a.payplan_id = g.payplan_id
                        LEFT JOIN xocp_orgs AS h ON a.org_id = h.org_id
                        LEFT JOIN xocp_his_patient AS z ON a.patient_id = z.patient_id
                        LEFT JOIN xocp_persons psn on psn.person_id = z.person_id
                        LEFT JOIN xocp_his_patient_admission adm on adm.patient_id = a.patient_id and adm.admission_id = a.admission_id
                        LEFT JOIN xocp_his_clin_priv priv on b.clin_priv_id = priv.clin_priv_id
                        LEFT JOIN xocp_obj AS obj ON priv.role_obj_id = obj.obj_id
                        WHERE
                        a.ordered_dttm >= '2024-12-29 00:00:00' AND a.ordered_dttm <= '2024-12-31 23:59:59'
                        -- a.ordered_dttm >=  AND a.ordered_dttm <=
                        -- OR 
                        -- (a.updated_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 2 DAY), "%%Y-%%m-%%d 00:00:00") AND a.updated_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 23:59:59")))
                        AND a.obj_id NOT IN (0, '', 40904, 36171) -- filter yang OBJ bukan administrasi RS
                        AND a.payplan_id != '71'
                        -- AND a.status_cd = 'normal'
                        AND priv.role_obj_id != 42386 -- filter yg bukan JPND
                        AND b.employee_id != 0
                        -- SEMENTARA!!!!!!!! GANTI KE EMPLOYEE ID 1550
                        -- AND b.employee_id = 1550
                        -- AND a.order_id IN (159697055)
                        ORDER BY a.order_id
                        """
    source = pd.read_sql_query(query_source,conn_his_live)

    source['ObjID'] = source['ObjID'].astype('str')
 

    for i in source.index:
            cat = source.loc[i, 'CategoryName']
            obj = source.loc[i, 'ObjName'].lower()
            if cat == 'tindakan':
                if ('usg' in obj) | (obj.startswith('ct ')) | (obj.startswith('mri ')) | ('sgpt' in obj) \
                    | ('sgot' in obj) | ('osmolaritas plasma' in obj) | ('ferritin' in obj) | ('ultrasound' in obj) \
                    | ('ekg' in obj) | ('eeg' in obj) | ('tinja' in obj) | ('trigli' in obj) \
                    | ('puasa' in obj) | (obj.startswith('anti ')) | ('gliko' in obj) | ('hb' in obj) | ('sputum' in obj) \
                    | ('transportasi' in obj) | (obj.startswith('kain laken ')) | ('administrasi' in obj)  :
                    source.loc[i, 'CategoryName'] = 'penunjang'

    
        # bikin function remove comma jika ada variable yang isinya cuma 1, variable ini akan di pakai IN CLAUSE di query target
    def remove_comma(x):
        if len(x) == 1:
            return str(x).replace(',','')
        else:
            return x
    employeeid = tuple(source['EmployeeID'].unique())
    employeeid = remove_comma(employeeid)

    get_doctor = f""" SELECT
                        a.EmployeeID,
                        a.EmployeeName as DoctorName,
                        a.EmployeeNo as NIP,
                        a.NIK,
                        b.ChildOrganizationName KSM
                    FROM dwhrscm_talend.DimEmployee a
                    LEFT JOIN dwhrscm_talend.DimOrganization b on a.UnitOrgID = b.ChildOrganizationID and b.SCDActive = '1'
                    WHERE a.SCDActive = '1'
                    and a.EmployeeID IN {employeeid} """
    query_get_doctor = pd.read_sql_query(get_doctor,conn_dwh_sqlserver)

    # # lalu di joinkan ke tabel hrm_emp_idcard untuk mendapatkan NIK
    # get_nik = f""" SELECT employee_id EmployeeID, id_card_num as NIK FROM xocp_hrm_emp_idcard 
    #                                     WHERE idcard_type = '1' and employee_id IN {employeeid}
    #                                     GROUP BY employee_id """
    # query_get_nik = pd.read_sql_query(get_nik, conn_ehr)

    # join semua source dari source awal, query_get_doctor,query_get_nik, query_get_ksm
    source= source.merge(query_get_doctor,how='left',on='EmployeeID')
    print(source)
    # bikin query untuk ambil kolom ObjectGroupingName
    query_object_grouping = f""" SELECT ObjectID as ObjID ,ObjectGroupingName 
                                FROM [dwhrscm_talend].[DimObjectGroupingDetail] dt
                                LEFT join dwhrscm_talend.DimObjectGroupingMaster dm on dt.ObjectGroupingID = dm.ObjectGroupingD
                                WHERE dt.SCDActive = 1 
                            """
    object_grouping = pd.read_sql_query(query_object_grouping, conn_dwh_sqlserver)
    
    source = source.merge(object_grouping,how='left',on='ObjID')

    # adjust kembali urutan kolomnya seperti berikut
    new_order_columns = ['OrderID','PatientID','AdmissionID','EmployeeID','DoctorName','KSM','NIP','NIK','PatientName','MedicalNo','SEPNo',
                         'OrgName','AdmissionDate','OrderDate','NullifiedDate','VerifiedDate','ObjID','ObjName','ObjRoleName','RoleNo',
                        'IDItem','MonthValue','YearValue','Tarif','JasaMedis','JasaSarana','JasaRemun','PayplanName',
                        'CategoryName','PayplanKemkes','Type','Status','ObjectGroupingName','BillingPaid']

    source = source.reindex(columns=new_order_columns)

    # transformasi data untuk kolom NIK NIP dan DoctorName ubah jadi - untuk yg EmployeeID nya 0 dan 1
    source.loc[source['EmployeeID'] == 0, 'NIK'] = '-'
    source.loc[source['EmployeeID'] == 1, 'NIP'] = '-'
    source.loc[source['EmployeeID'] == 1, 'DoctorName'] = '-'

    # ubah tipe data pada kolom2 tertentu menyesuaikan dengan yang ada di tabel
    source['EmployeeID'] = source['EmployeeID'].fillna('0').astype('int64')
    source['DoctorName'] = source['DoctorName'].fillna('-')
    source['KSM'] = source['KSM'].fillna('-')
    source['NIP'] = source['NIP'].fillna('-')
    source['NIK'] = source['NIK'].fillna('-')
    source['Tarif'] = source['Tarif'].fillna('0')
    source['JasaMedis'] = source['JasaMedis'].fillna('0')
    source['MonthValue'] = source['MonthValue'].astype('str')
    source['YearValue'] = source['YearValue'].astype('str')
    source['Tarif'] = source['Tarif'].astype('float64').round(2)
    source['JasaMedis'] = source['JasaMedis'].astype('float64').round(2)
    source['JasaSarana'] = source['JasaSarana'].astype('float64')
    source['OrderID'] = source['OrderID'].astype('str')
    source['ObjID'] = source['ObjID'].astype('str')
    source['NullifiedDate'] = source['NullifiedDate'].astype('datetime64')
    

    # filter yang EmployeeID nya bukan 0
    source = source[source['EmployeeID']!= 0]

    # hitung jasa sarana
    jasa_sarana = source.groupby('OrderID')['JasaMedis'].transform('sum')
    source['JasaSarana'] = np.where(source['Tarif'] == 0, 0, source['Tarif'] - jasa_sarana)

    return source

def get_target_data(source, conn_dwh_sqlserver):
    """ ambil data dari tabel target, yaitu FactOrderBasedNew """

    # masukkan data tarikan query ke tabel temporary, 
    source.to_sql('FactOrderBasedNewTemporary', schema='dwhrscm_talend',con=conn_dwh_sqlserver,if_exists='replace', index=False)
    query_filter = f"""SELECT OrderID,PatientID,AdmissionID,EmployeeID,DoctorName,KSM,NIP,NIK,PatientName,MedicalNo,SEPNo,OrgName,AdmissionDate,
                        OrderDate,NullifiedDate,VerifiedDate,ObjID,ObjName,ObjRoleName,RoleNo,IDItem,MonthValue,YearValue,Tarif,JasaMedis,JasaSarana,
                        JasaRemun,PayplanName,CategoryName,PayplanKemkes,Type,Status,ObjectGroupingName,BillingPaid
                        FROM dwhrscm_talend.FactOrderBasedNew where OrderID IN (SELECT OrderID FROM dwhrscm_talend.FactOrderBasedNewTemporary) 
                        ORDER BY OrderID"""
    target = pd.read_sql_query(query_filter,conn_dwh_sqlserver)
    target['NullifiedDate'] = target['NullifiedDate'].astype('datetime64')
    target['Tarif'] = target['Tarif'].astype('float64')

    query_drop_table = f"DROP TABLE dwhrscm_talend.FactOrderBasedNewTemporary"
    conn_dwh_sqlserver.execute(query_drop_table)
    target['ObjectGroupingName'].replace({np.nan: None},inplace=True)
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
    """ Insert data di tabel FactOrderBasedNew"""
    if not inserted.empty:
        with conn_dwh_sqlserver.begin() as transaction:
            inserted.to_sql('FactOrderBasedNew', schema='dwhrscm_talend', con=conn_dwh_sqlserver, if_exists='append', index=False)
            print('Data Success Inserted')
    else:
        print('Tidak ada data yang baru')

def main():
    """ Fungsi utama untuk menjalankan semua proses"""

    conn_his_live ,conn_his,conn_ehr, conn_dwh_sqlserver, conn_staging_sqlserver = get_connections()

    try:
         # Ambil data dari source
        source = get_source_data(conn_his_live, conn_ehr,conn_dwh_sqlserver)
        print("Source Data:")
        print(source)
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
        print(inserted)

        # update data
        updated_data(modified, 'FactOrderBasedNew', 'OrderID','RoleNo', conn_dwh_sqlserver)

        # insert data
        inserted_data(inserted, conn_dwh_sqlserver)
    finally:
        db_connection.close_connection(conn_his_live)
        db_connection.close_connection(conn_his)
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
