import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pyodbc
import pandas as pd
import numpy as np
import time
import datetime as dt
date = dt.datetime.today()
from datetime import datetime,timedelta
import sys

sys.stdout = open("C:/TestPython/DWH_SQL_Server/DWH/logs/LogFactOrderBasedHISRealtime.txt","w")
t0 = time.time()

conn_his = db_connection.create_connection(db_connection.replika_his)
conn_ehr = db_connection.create_connection(db_connection.replika_ehr)
conn_dwh_sqlserver = db_connection.create_connection(db_connection.dwh_sqlserver)

# bikin variabel start_date dan end_date untuk memasukkan range tanggal awal dan akhir untuk diletakkan di WHERE query source 1,2,3
start_date = f"DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), '%%Y-%%m-%%d 00:00:00')"
end_date = f"DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), '%%Y-%%m-%%d 23:59:59')"

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
                                psn.person_nm as PatientName,
                                adm.admission_dttm as AdmissionDate,
                                a.ordered_dttm as OrderDate,
                                a.nullified_dttm as NullifiedDate,
                                NULL as VerifiedDate,
                                z.patient_mrn_txt as MedicalNo,
                                h.org_nm as OrgName,
                                a.obj_id as ObjID,
                                c.obj_nm as ObjName,
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
                                'K' AS Type
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
                            WHERE
                            -- a.ordered_dttm >= '2024-01-21 00:00:00' AND a.ordered_dttm <= '2024-01-25 23:59:59'
                            (a.ordered_dttm >= {start_date} AND a.ordered_dttm <= {end_date})
                            -- OR 
                            -- (a.updated_dttm >= {start_date} AND a.updated_dttm <= {end_date})
                            -- AND a.status_cd = 'normal' 
                            AND a.obj_id NOT IN ('0', '', '40904', '36171') 
                            AND a.payplan_id != '71'
                            AND a.status_cd = 'normal'
                            -- AND a.order_id IN (155740400)
                            ORDER BY a.order_id
                            """
source = pd.read_sql_query(query_source,conn_his)
print(source)

employeeid = tuple(source['EmployeeID'])

# ambil nama doktor, NIP dan UnitOrg dari tabel hrm_employee EHR menggunakan EmployeeID
get_doctor = f"""SELECT employee_id as EmployeeID, employee_unitorg_id as UnitOrg, person_nm as DoctorName , employee_ext_id as NIP
                    FROM xocp_hrm_employee WHERE employee_id IN {employeeid}"""
query_get_doctor = pd.read_sql_query(get_doctor, conn_ehr)

# lalu di joinkan ke tabel hrm_emp_idcard untuk mendapatkan NIK
get_nik = f""" SELECT employee_id EmployeeID, id_card_num as NIK FROM xocp_hrm_emp_idcard 
                                    WHERE idcard_type = '1' and employee_id IN {employeeid}
                                    GROUP BY employee_id """
query_get_nik = pd.read_sql_query(get_nik, conn_ehr)

# lalu di joinkan untuk mendapatkan nama KSM dokter
query_get_ksm = pd.read_sql_query(""" SELECT org_id as UnitOrg, org_nm as KSM 
                                    FROM xocp_orgs """, conn_ehr)

# join semua source dari source awal, query_get_doctor,query_get_nik, query_get_ksm
source= source.merge(query_get_doctor,how='left',on='EmployeeID').merge(query_get_nik,how='left',on='EmployeeID').merge(query_get_ksm, how='left',on='UnitOrg')

# adjust kembali urutan kolomnya seperti berikut
new_order_columns = ['OrderID','PatientID','AdmissionID','EmployeeID','DoctorName','KSM','NIP','NIK','PatientName','MedicalNo','OrgName',
                    'AdmissionDate','OrderDate','NullifiedDate','VerifiedDate','ObjID','ObjName','RoleNo',
                    'IDItem','MonthValue','YearValue','Tarif','JasaMedis','JasaSarana','JasaRemun','PayplanName',
                    'CategoryName','PayplanKemkes','Type']

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
source['Tarif'] = source['Tarif'].astype('float64')
source['JasaMedis'] = source['JasaMedis'].astype('float64')
source['JasaSarana'] = source['JasaSarana'].astype('float64')
source['OrderID'] = source['OrderID'].astype('str')
source['ObjID'] = source['ObjID'].astype('str')
print(source.dtypes)

# filter yang EmployeeID nya bukan 0
source = source[source['EmployeeID']!= 0]

# hitung jasa sarana
jasa_sarana = source.groupby('OrderID')['JasaMedis'].transform('sum')
source['JasaSarana'] = source['Tarif'] - jasa_sarana

# # filter yang OrderID nya normal dari kolom NullifiedDate, filter yang null
# source = source[source['NullifiedDate'].isna()]

# jika data tarikan dari query kosong, maka tidak akan lanjut ke proses berikutnya
if source.empty:
    print('tidak ada data dari source')
# masukkan data tarikan query ke tabel temporary, 
# lalu kita bikin query untuk menarik data dari target dengan filter OrderID yang sudah ditarik dari source
else:
    source.to_sql('FactOrderBasedNewTemporary', schema='dwhrscm_talend',con=conn_dwh_sqlserver,if_exists='append', index=False)
    query_filter = f"""SELECT OrderID,PatientID,AdmissionID,EmployeeID,DoctorName,KSM,NIP,NIK,PatientName,MedicalNo,OrgName,AdmissionDate,OrderDate,NullifiedDate,VerifiedDate,ObjID,ObjName,RoleNo,IDItem,MonthValue,YearValue,Tarif,JasaMedis,JasaSarana,JasaRemun,PayplanName,CategoryName,PayplanKemkes,Type 
    FROM dwhrscm_talend.FactOrderBasedNew 
    WHERE OrderID IN (SELECT OrderID FROM dwhrscm_talend.FactOrderBasedNewTemporary) 
    AND MedicalNo NOT IN (SELECT MedicalNo FROM StagingRSCM.staging_rscm.DimensionDummyPatient)
    ORDER BY OrderID"""
    target = pd.read_sql_query(query_filter,conn_dwh_sqlserver)

query_drop_table = f"DROP TABLE dwhrscm_talend.FactOrderBasedNewTemporary"
conn_dwh_sqlserver.execute(query_drop_table)
print(target)

# ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

# ambil data yang update dari changes
modified = changes[changes[['OrderID','RoleNo']].apply(tuple,1).isin(target[['OrderID','RoleNo']].apply(tuple,1))]
total_row_upd = len(modified)
text_upd = f'total row update : {total_row_upd}'
print(text_upd)
print(modified.iloc[:,0:6])

# ambil data yang new dari changes
inserted = changes[~changes[['OrderID','RoleNo']].apply(tuple,1).isin(target[['OrderID','RoleNo']].apply(tuple,1))]
total_row_ins = len(inserted)
text_ins = f'total row inserted : {total_row_ins}'
print(text_ins)
print(inserted.iloc[:,0:6])

# jika tidak ada data yang update, maka data yang baru akan langsung di insert ke tabel
if modified.empty:
    inserted.to_sql('FactOrderBasedNew', schema = 'dwhrscm_talend',con = conn_dwh_sqlserver, if_exists = 'append', index=False)
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
        df.to_sql(temp_table,schema='dwhrscm_talend',con=conn_dwh_sqlserver, if_exists='replace',index=False)
        update_stmt_1 = f'UPDATE r '
        update_stmt_2 = f'SET '
        update_stmt_3 = ", ".join(list_col)
        update_stmt_4 = f' FROM dwhrscm_talend.{table} r '
        update_stmt_5 = f'INNER JOIN (SELECT * FROM dwhrscm_talend.{temp_table}) as t ON r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} '
        update_stmt_6 = f'WHERE r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} '
        update_stmt_7 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_4 + update_stmt_5 + update_stmt_6 +";"
        delete_stmt_1 = f'DROP TABLE dwhrscm_talend.{temp_table}'
        print(update_stmt_7)
        conn_dwh_sqlserver.execute(update_stmt_7)
        conn_dwh_sqlserver.execute(delete_stmt_1)

    try:
        # update data
        updated_to_sql(modified, 'FactOrderBasedNew', 'OrderID','RoleNo')
        inserted.to_sql('FactOrderBasedNew', schema='dwhrscm_talend',con=conn_dwh_sqlserver, if_exists ='append',index=False)
        print('success update and insert all data\n')
    
    except Exception as e:
        print(e)

#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)

text=f'scheduler tanggal : {date}'
print(text)

# conn_ehr.close()
db_connection.close_connection(conn_ehr)
db_connection.close_connection(conn_dwh_sqlserver)
db_connection.close_connection(conn_his)