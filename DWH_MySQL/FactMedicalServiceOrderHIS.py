import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pyodbc
import pandas as pd
import numpy as np
import time
import datetime as dt
date = dt.datetime.today()
import sys

sys.stdout = open("C:/TestPython/DWH_MySQL/logs/LogFactMedicalServiceOrderHIS.txt","w")
t0 = time.time()

conn_ehr = db_connection.create_connection(db_connection.replika_ehr)
conn_dwh_mysql = db_connection.create_connection(db_connection.dwh_mysql)
conn_his = db_connection.create_connection(db_connection.replika_his)

source = pd.read_sql_query("""
                              SELECT 
                                    a.order_id as OrderID,
                                    adm.admission_dttm as AdmissionDate,
                                    a.ordered_dttm as OrderDate,
                                    z.patient_mrn_txt as MedicalNo,
                                    h.org_nm as OrgName,
                                    a.obj_id as ObjID,
                                    c.obj_nm as ObjName,
                                    CASE 
                                        WHEN b.role_no IS NULL THEN '-'
                                        ELSE b.role_no
                                    END AS RoleNo,
                                    f.tariff as Tarif,
                                    CASE
                                        WHEN b.tariff - b.discount IS NULL THEN '-'
                                        ELSE b.tariff - b.discount
                                    END AS JM,
                                    g.payplan_nm as PayplanName,
                                    CASE
                                        WHEN b.employee_id IS NULL THEN 0
                                        ELSE b.employee_id
                                    END AS EmployeeID,
                                    CASE 
                                            WHEN c.obj_nm LIKE '%%Konsul%%' THEN 'jalan'
                                            WHEN c.obj_nm LIKE '%%Visit%%' THEN 'visitasi'
                                            WHEN h.org_nm LIKE '%%Lab%%'  THEN 'penunjang'
                                            WHEN h.org_nm LIKE '%%Radiologi%%' THEN 'penunjang'
                                            ELSE 'tindakan'
                                    END AS CategoryName,
                                    'Eksekutif' AS PayplanKemkes,
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
                                LEFT JOIN xocp_his_patient_admission adm on adm.patient_id = a.patient_id and adm.admission_id = a.admission_id
                                WHERE
                                ((a.ordered_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 3 DAY), "%%Y-%%m-%%d 00:00:00") AND a.ordered_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 23:59:59"))
                                OR 
                                (a.updated_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 00:00:00") AND a.updated_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 23:59:59")))
                                AND a.status_cd = 'normal' 
                                AND a.obj_id NOT IN ('0', '', '40904', '36171') 
                                AND a.payplan_id != '71'
                                -- AND a.order_id IN (145979831,145980923)
                                ORDER BY a.order_id
                                """, conn_his)

query_get_doctor = pd.read_sql_query(""" SELECT employee_id as EmployeeID, person_nm as PersonName 
                                    FROM xocp_hrm_employee """, conn_ehr)
query_get_nik = pd.read_sql_query(""" SELECT employee_id EmployeeID, id_card_num as NIK FROM xocp_hrm_emp_idcard 
                                    WHERE idcard_type = '1' GROUP BY employee_id """,conn_ehr)

source= source.merge(query_get_doctor,how='left',on='EmployeeID').merge(query_get_nik,how='left',on='EmployeeID')

new_order_columns = ['OrderID','AdmissionDate','OrderDate','MedicalNo','OrgName','ObjID','ObjName','RoleNo','Tarif',
                    'JM','PayplanName','EmployeeID','PersonName','NIK','CategoryName','PayplanKemkes','Type']
                    
source = source.reindex(columns=new_order_columns)
source['EmployeeID'] = source['EmployeeID'].fillna('0').astype('int64')
source['PersonName'] = source['PersonName'].fillna('-')
print(source)
# source = source[(source['Tarif'] != '0') & (source['JM'] != '0')]
print(source.iloc[:,0:10])
source['OrderID'] = source['OrderID'].astype('str')
source['ObjID'] = source['ObjID'].astype('str')
print(source.dtypes)

# source=source.fillna('-')
# source['JasaMedis']=source['JasaMedis'].astype(str)
# source.replace({np.nan: None},inplace=True)
# print(source.dtypes)
# print(source.isna().any())

if source.empty:
    print('tidak ada data dari source')
else:
    # jika dari source cuma 1 row
    if len(source) == 1:        
        # ambil primary key dari source, ambil index ke 0
        orderid = source["OrderID"].values[0]
        roleno = source=["RoleNo"].values[0]

        # query buat narik data dari target lalu filter berdasarkan primary key
        query = f"SELECT OrderID,AdmissionDate,OrderDate,MedicalNo,OrgName,ObjID,ObjName,RoleNo,Tarif,JM,PayplanName,EmployeeID,PersonName,NIK,CategoryName,PayplanKemkes,Type FROM FactMedicalServiceOrder where OrderID IN ({orderid}) AND RoleNo IN ({roleno}) ORDER BY OrderID, RoleNo"
        target = pd.read_sql_query(query, conn_dwh_mysql)
    else :
         # ambil primary key dari source, pake unique biar tidak duplicate
        orderid = tuple(source["OrderID"].unique())
        roleno = tuple(source["RoleNo"])

        # query buat narik data dari target lalu filter berdasarkan primary key
        query = f"SELECT OrderID,AdmissionDate,OrderDate,MedicalNo,OrgName,ObjID,ObjName,RoleNo,Tarif,JM,PayplanName,EmployeeID,PersonName,NIK,CategoryName,PayplanKemkes,Type FROM FactMedicalServiceOrder where OrderID IN {orderid} AND RoleNo IN {roleno} ORDER BY OrderID, RoleNo"
        target = pd.read_sql_query(query, conn_dwh_mysql)

    # cek tipe data target
    print(target.dtypes)

    # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]
    # print(source.apply(tuple,1).isin(target.apply(tuple,1)))
    # print(source.iloc[:,[17,19]])
    # print(target.iloc[:,[17,19]])
    # print(source.iloc[:,0:4].apply(tuple,1))
    # print(target.iloc[:,0:4].apply(tuple,1))
    # print(source.iloc[:,3:5].apply(tuple,1))
    # print(target.iloc[:,3:5].apply(tuple,1))
    # print(source.iloc[:,5:7].apply(tuple,1))
    # print(target.iloc[:,5:7].apply(tuple,1))
    # print(source.iloc[:,6:10].apply(tuple,1))
    # print(target.iloc[:,6:10].apply(tuple,1))
    # print(source.iloc[:,9:13].apply(tuple,1))
    # print(target.iloc[:,9:13].apply(tuple,1))
    # print(source.iloc[:,12:16].apply(tuple,1))
    # print(target.iloc[:,12:16].apply(tuple,1))
    # print(source.iloc[:,14:19].apply(tuple,1))
    # print(target.iloc[:,14:19].apply(tuple,1))
    # print(source.iloc[:,18:22].apply(tuple,1))
    # print(target.iloc[:,18:22].apply(tuple,1))
    # print(source.iloc[:,21:26].apply(tuple,1))
    # print(target.iloc[:,21:26].apply(tuple,1))
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

    if modified.empty:
        inserted.to_sql('FactMedicalServiceOrder', con = conn_dwh_mysql, if_exists = 'append', index=False)
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
            df.to_sql(temp_table,con=conn_dwh_mysql, if_exists='replace',index=False)
            update_stmt_1 = f'UPDATE {table} r '
            update_stmt_2 = f'INNER JOIN (SELECT * FROM {temp_table}) as t ON r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} '
            update_stmt_3 = f'SET '
            update_stmt_4 = ", ".join(list_col)
            update_stmt_5 = f' WHERE r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} '
            update_stmt_6 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_4 + update_stmt_5 +";"
            delete_stmt_1 = f'DROP TABLE {temp_table}'
            print(update_stmt_6)
            conn_dwh_mysql.execute(update_stmt_6)
            conn_dwh_mysql.execute(delete_stmt_1)

        try:
            # update data
            updated_to_sql(modified, 'FactMedicalServiceOrder', 'OrderID','RoleNo')
            inserted.to_sql('FactMedicalServiceOrder', con=conn_dwh_mysql, if_exists ='append',index=False)
            print('success update and insert all data')
        
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
db_connection.close_connection(conn_dwh_mysql)
db_connection.close_connection(conn_his)