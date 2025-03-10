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

sys.stdout = open("C:/TestPython/DWH_MySQL/logs/LogFactMedicalServiceOrderEHR.txt","w")
t0 = time.time()

conn_ehr = db_connection.create_connection(db_connection.replika_ehr)
conn_dwh_mysql = db_connection.create_connection(db_connection.dwh_mysql)

source = pd.read_sql_query("""
                               SELECT 
                                    x.order_id as OrderID,
                                    x.admission_dttm as AdmissionDate,
                                    x.ordered_dttm as OrderDate,
                                    x.patient_ext_id as MedicalNo,
                                    x.org_nm as OrgName,
                                    x.obj_id_real as ObjID,
                                    x.obj_nm as ObjName,
                                    x.role_no as RoleNo,
                                    x.tariff as Tarif,
                                    CASE 
                                        WHEN x.JM IS NULL THEN '-'
                                        ELSE x.JM
                                    END AS JM,
                                    x.payplan_nm as PayplanName,
                                    CASe
                                        WHEN emp.employee_id IS NULL THEN 0
                                        ELSE emp.employee_id
                                    END AS EmployeeID,
                                    CASE 
                                        WHEN emp.person_nm IS NULL THEN '-'
                                        ELSE emp.person_nm 
                                    END AS PersonName,
                                    CASE 
                                        WHEN idcard.id_card_num IS NULL THEN '-'
                                        ELSE idcard.id_card_num
                                    END AS NIK,
                                    CASE 
                                        WHEN x.obj_nm LIKE '%%Konsul%%' THEN 'jalan'
                                        WHEN x.obj_nm LIKE '%%Visit%%' THEN 'visitasi'
                                        WHEN x.org_nm LIKE '%%Lab%%'  THEN 'penunjang'
                                        WHEN x.org_nm LIKE '%%Radiologi%%' THEN 'penunjang'
                                    ELSE 'tindakan'
                                    END AS CategoryName,
                                    CASE
                                        WHEN x.payplan_id = 71 THEN 'BPJS'
                                        ELSE 'Non BPJS'
                                    END AS PayplanKemkes,
                                    'P' as Type
                                    FROM 
                                    (
                                        SELECT 
                                            a.order_id,
                                            a.ordered_dttm,
                                            e.admission_dttm,
                                            CASE
                                                WHEN c.role_no IS NULL THEN '-'
                                                ELSE c.role_no
                                            END AS role_no,
                                            f.patient_ext_id,
                                            a.obj_id as obj_id_real,
                                            i.org_nm,
                                            g.person_nm,
                                            d.obj_nm,
                                            b.tariff,
                                            c.tariff - c.disc_tariff AS JM,
                                            a.payplan_id,
                                            h.payplan_nm,
                                            CASE
                                                    WHEN c.obj_id LIKE '%%.%%' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(c.obj_id,'.',2),'.',-1)
                                                    ELSE '-'
                                            END AS obj_id
                                        FROM
                                        xocp_ehr_patient_order AS a
                                        LEFT JOIN xocp_ehr_payplan_obj AS b ON a.obj_id = b.obj_id AND a.payplan_id = b.payplan_id
                                        LEFT JOIN xocp_ehr_patient_order_role AS c ON a.order_id = c.order_id AND a.payplan_id = c.payplan_id
                                        LEFT JOIN xocp_ehr_obj AS d ON a.obj_id = d.obj_id
                                        LEFT JOIN xocp_ehr_patient_admission AS e ON a.patient_id = e.patient_id AND a.admission_id = e.admission_id
                                        LEFT JOIN xocp_ehr_patient AS f ON e.patient_id = f.patient_id
                                        LEFT JOIN xocp_persons AS g ON f.person_id = g.person_id
                                        LEFT JOIN xocp_ehr_payplan AS h ON a.payplan_id = h.payplan_id
                                        LEFT JOIN xocp_orgs as i on a.hcp_id = i.org_id
                                        WHERE
                                        (a.ordered_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 3 DAY), "%%Y-%%m-%%d 00:00:00") 
                                        AND a.ordered_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 23:59:59"))
                                        AND a.status_cd = 'new' 
                                        AND a.obj_id NOT IN ('00200003213234', '00200003213235','MDCTx000000056','MDCTx000000057','MSRVx000000200',
                                        'MSRVx000000202','MSRVx000000234','MSRVx000000260','MSRVx000000261','PROCx000032315','PROCx000014192',
                                        'PROCx000017209','PROCx000031516')
                                    ) x
                                    LEFT JOIN xocp_hrm_employee emp on emp.employee_id = x.obj_id
                                    LEFT JOIN (SELECT employee_id, id_card_num FROM xocp_hrm_emp_idcard WHERE idcard_type = '1' GROUP BY employee_id) idcard on emp.employee_id = idcard.employee_id
                                """, conn_ehr)

print(source)
# source = source[(source['Tarif'] != '0') & (source['JM'] != '0')]
print(source.iloc[:,0:10])
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
        roleno = source=["roleno"].values[0]

        # query buat narik data dari target lalu filter berdasarkan primary key
        query = f"SELECT OrderID,AdmissionDate,OrderDate,MedicalNo,OrgName,ObjID,ObjName,RoleNo,Tarif,JM,PayplanName,EmployeeID,PersonName,NIK,CategoryName,PayplanKemkes,Type FROM FactMedicalServiceOrder where OrderID IN ({orderid}) AND RoleNo IN ({roleno}) ORDER BY OrderID, RoleNo"
        target = pd.read_sql_query(query, conn_dwh_mysql)
    else :
         # ambil primary key dari source, pake unique biar tidak duplicate
        orderid = tuple(source["OrderID"].unique())
        roleno = tuple(source["RoleNo"].unique())

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
