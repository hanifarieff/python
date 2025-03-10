import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pyodbc
import pandas as pd
import numpy as np
import time
import datetime as dt
from datetime import datetime, timedelta
date = dt.datetime.today()

sys.stdout = open("C:/TestPython/DWH_SQL_Server/DWH/logs/LogFactRemunBased.txt","w")
t0 = time.time()

# conn_ehr_live = db_connection.create_connection(db_connection.ehr_live)
conn_ehr = db_connection.create_connection(db_connection.replika_ehr)
conn_his = db_connection.create_connection(db_connection.his_live)
conn_dwh_sqlserver = db_connection.create_connection(db_connection.dwh_sqlserver)

first_day_of_current_month = date.replace(day=1)
last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
# previous_month = last_day_of_previous_month.strftime('%m')
# previous_year = last_day_of_previous_month.strftime('%Y')
previous_month = '12'
previous_year = 2024

query_source = f""" SELECT 
                                    b.order_id OrderID,
                                    b.employee_id EmployeeID,
                                    c.person_nm as DoctorName,
                                    j.org_nm as KSM,
                                    c.employee_ext_id as NIP,
                                    k.id_card_num as NIK,
                                    b.patient_nm as PatientName,
                                    b.patient_ext_id as MedicalNo,
                                    b.sep_no as SEPNo,
									org.org_nm as OrgName,
                                    b.admission_dttm AdmissionDate,
                                    b.ordered_dttm as OrderDate,
                                    NULL as NullifiedDate,
                                    NULL AS VerifiedDate,
                                    e.obj_id as ObjID,
                                    b.proc_nm as ObjName,
                                    m.obj_nm as ObjRoleName,
                                    NULL AS RoleNo,
                                    CASE
                                        WHEN b.item_id IS NULL THEN '-'
                                        ELSE b.item_id 
                                    END AS IDItem,
                                    b.bulan as MonthValue,
                                    b.tahun as YearValue,
                                    CASE
                                        WHEN z.tariff IS NULL or z.tariff = '0' or z.tariff = '' THEN CAST(d.tariff as DECIMAL(15,2))
                                        ELSE CAST(z.tariff as DECIMAL(15,2))
                                    END as Tarif,
                                    CAST(b.jasa_medis as DECIMAL(15,2)) as JasaMedis,
                                    CAST(b.jasa_remun as DECIMAL(15,2)) as JasaRemun,
                                    b.payplan_nm as PayplanName,
                                    CASE
                                        WHEN b.remun_cat IS NULL THEN '-'
                                        ELSE b.remun_cat 
                                    END AS CategoryName,
                                    CASE
                                        WHEN b.flag_pf = 'f' THEN 'FFS'
                                        ELSE 'FFP'
                                    END AS PayplanKemkes,
                                    CASE
                                        WHEN b.payplan_nm IN ('JAMINAN KESEHATAN NASIONAL','JAMKESMAS','JAMINAN DIREKSI','Jaminan COVID-19','JAMKESDA','PUSAT KRISIS TERPADU (PKT)','MCU Pegawai') THEN 'JKN'
                                        ELSE 'Non JKN'
                                    END AS PayplanType,
                                    'Kepdirjend' AS Type,
                                    CASE 
                                        WHEN b.proc_nm LIKE '%%Konsul%%' THEN 'jalan'
                                        WHEN b.proc_nm LIKE '%%Visit%%' THEN 'visitasi'
                                        WHEN b.hcp_nm LIKE '%%Lab%%'  THEN 'penunjang'
                                        WHEN b.hcp_nm LIKE '%%Radiologi%%' THEN 'penunjang'
                                        ELSE 'tindakan'
                                    END AS CategoryNameNew,
                                    CASE
                                        WHEN b.order_id NOT LIKE '00%%' THEN 
                                            CASE 
                                                WHEN b.payplan_nm IN ('PRIBADI','UMUM') THEN 'Eksekutif'
                                                ELSE 'Eksekutif Jaminan'
                                            END
                                        WHEN b.order_id LIKE '00%%' THEN
                                            CASE
                                                WHEN b.payplan_nm IN ('JAMINAN KESEHATAN NASIONAL','JAMKESMAS','JAMINAN DIREKSI','Jaminan COVID-19','JAMKESDA','PUSAT KRISIS TERPADU (PKT)','MCU Pegawai') THEN 'JKN'
                                                ELSE 'Non JKN'
                                            END
                                    END AS PayplanKemkesNew
                            FROM xocp_hrm_insentifdpjp a
                            INNER JOIN xocp_hrm_insentifdpjp_item b on a.poin_id = b.poin_id and a.bulan = b.bulan AND a.tahun = b.tahun and a.employee_id = b.employee_id 
                            LEFT JOIN xocp_hrm_employee c on a.employee_id = c.employee_id
                            LEFT JOIN xocp_ehr_patient_order d on b.order_id = d.order_id
                            LEFT JOIN xocp_ehr_payplan_obj z on d.obj_id = z.obj_id and d.payplan_id = z.payplan_id
                            LEFT JOIN xocp_ehr_obj e on d.obj_id = e.obj_id
                            LEFT JOIN xocp_orgs j on c.employee_unitorg_id = j.org_id
                            LEFT JOIN (SELECT employee_id, id_card_num FROM xocp_hrm_emp_idcard WHERE idcard_type = '1' GROUP BY employee_id) k on c.employee_id = k.employee_id
                            LEFT JOIN xocp_ehr_patient_order_role l on d.order_id = l.order_id and d.payplan_id = l.payplan_id and l.role_no = b.role_no
                            LEFT JOIN xocp_ehr_obj m on m.obj_id = l.default_obj_id
                            -- LEFT JOIN xocp_ehr_patient_admission adm on adm.patient_id = d.patient_id and 
                            -- adm.admission_id = d.admission_id and adm.status_cd = 'normal'
                            LEFT JOIN xocp_orgs org on org.org_id = d.client_id
                            WHERE a.bulan = '01' and a.tahun = '2025' and a.status_cd != 'nullified' -- ganti bulan menyesuaikan kebutuhan yg diminta, 08 berarti agustus.
                            and c.status_cd = 'active'
                            -- AND b.order_id IN ('00200007106335')
                            and b.order_id LIKE '00%%'
                            -- AND b.order_id = '00190006464844'
                            ORDER BY b.order_id, a.employee_id """
source = pd.read_sql_query(query_source, conn_ehr)

# untuk kolom SEPNo yang null dan string kosong, ubah jadi strip
source['SEPNo'].replace({np.nan: '-', '': '-'}, inplace=True)

# ini ga kepake
# source = source[(source['Tarif'] != '0') & (source['JM'] != '0')]
# print(source.iloc[:,0:10])
# print(source.dtypes)
# source=source.fillna('-')
# source['JasaMedis']=source['JasaMedis'].astype(str)
# source.replace({np.nan: None},inplace=True)
print(source.dtypes)
# print(source.isna().any())

# Filter 'OrderID' yang dari kencana
filtered_order_id = source.loc[~source['OrderID'].str.startswith('00'), 'OrderID']

# bikin kondisi jika ada order_id dari kencana, maka ambil ObjectID nya
if not filtered_order_id.empty:

    # Ubah OrderID jadi tuple buat dipake di query 
    order_id_tuple = tuple(filtered_order_id)

    # Tarik data dari patient order his berdasarkan OrderID yang udah di filter
    query_patient_order = f"""
        SELECT 
            order_id as OrderID,
            obj_id as ObjectIDNull
        FROM xocp_his_patient_order
        WHERE order_id IN {order_id_tuple} """

    his_patient_order = pd.read_sql_query(query_patient_order, conn_his)

    # ubah kolom OrderID jadi tipe data string biar bisa di join sama dataframe source
    his_patient_order['OrderID']= his_patient_order['OrderID'].astype('str')

    # join ke source
    source = source.merge(his_patient_order,how='left',on='OrderID')

    # ubah tipe data ObjectIDNull dan hilangkan .0 di belakangnya
    source['ObjectIDNull'] = source['ObjectIDNull'].fillna('').astype('str').str.replace('.0', '')

    # ObjID yang kosong, kita replace pake kolom ObjectIDNull yang dari his_patient_order
    source['ObjID'].fillna(source['ObjectIDNull'],inplace=True)
else:
    # Handle the case where there are no filtered OrderIDs
    pass

new_order_columns = ['OrderID','EmployeeID','DoctorName','KSM','NIP','NIK','PatientName','MedicalNo','SEPNo','OrgName',
                     'AdmissionDate','OrderDate','NullifiedDate','VerifiedDate','ObjID','ObjName','ObjRoleName','RoleNo',
                     'IDItem','MonthValue','YearValue','Tarif','JasaMedis','JasaRemun','PayplanName',
                     'CategoryName','PayplanKemkes','PayplanType','Type','CategoryNameNew','PayplanKemkesNew']
source = source.reindex(columns=new_order_columns)
print('after')
source['IDItem'] = source['IDItem'].astype('int64')
print(source)
print(source.dtypes)

try:
    with conn_dwh_sqlserver.begin() as transaction:
        # chunksize_size = 1000
        source.to_sql('FactRemunBasedNew', schema='dwhrscm_talend',con=conn_dwh_sqlserver, if_exists ='append',index=False)
        print('success insert all data')
except Exception as e:
    print(e)


# if source.empty:
#     print('tidak ada data dari source')
# else:
#     # jika dari source cuma 1 row
#     if len(source) == 1:        
#         # ambil primary key dari source, ambil index ke 0
#         orderid = source["OrderID"].values[0]
#         roleno = source=["roleno"].values[0]

#         # query buat narik data dari target lalu filter berdasarkan primary key
#         query = f"SELECT OrderID,AdmissionDate,OrderDate,MedicalNo,OrgName,ObjID,ObjName,RoleNo,Tarif,JM,PayplanName,EmployeeID,PersonName,NIK,CategoryName,PayplanKemkes,Type FROM dwhrscm_talend.FactOrderBased where OrderID IN ({orderid}) AND RoleNo IN ({roleno}) ORDER BY OrderID, RoleNo"
#         target = pd.read_sql_query(query, conn_dwh_sqlserver)
#     else :
#          # ambil primary key dari source, pake unique biar tidak duplicate
#         orderid = tuple(source["OrderID"].unique())
#         roleno = tuple(source["RoleNo"].unique())

#         # query buat narik data dari target lalu filter berdasarkan primary key
#         query = f"SELECT OrderID,AdmissionDate,OrderDate,MedicalNo,OrgName,ObjID,ObjName,RoleNo,Tarif,JM,PayplanName,EmployeeID,PersonName,NIK,CategoryName,PayplanKemkes,Type FROM dwhrscm_talend.FactOrderBased where OrderID IN {orderid} AND RoleNo IN {roleno} ORDER BY OrderID, RoleNo"
#         target = pd.read_sql_query(query, conn_dwh_sqlserver)

#     # cek tipe data target
#     print(target.dtypes)

#     # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
#     changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]
#     # print(source.apply(tuple,1).isin(target.apply(tuple,1)))
#     # print(source.iloc[:,[17,19]])
#     # print(target.iloc[:,[17,19]])
#     # print(source.iloc[:,0:4].apply(tuple,1))
#     # print(target.iloc[:,0:4].apply(tuple,1))
#     # print(source.iloc[:,3:5].apply(tuple,1))
#     # print(target.iloc[:,3:5].apply(tuple,1))
#     # print(source.iloc[:,5:7].apply(tuple,1))
#     # print(target.iloc[:,5:7].apply(tuple,1))
#     # print(source.iloc[:,6:10].apply(tuple,1))
#     # print(target.iloc[:,6:10].apply(tuple,1))
#     # print(source.iloc[:,9:13].apply(tuple,1))
#     # print(target.iloc[:,9:13].apply(tuple,1))
#     # print(source.iloc[:,12:16].apply(tuple,1))
#     # print(target.iloc[:,12:16].apply(tuple,1))
#     # print(source.iloc[:,14:19].apply(tuple,1))
#     # print(target.iloc[:,14:19].apply(tuple,1))
#     # print(source.iloc[:,18:22].apply(tuple,1))
#     # print(target.iloc[:,18:22].apply(tuple,1))
#     # print(source.iloc[:,21:26].apply(tuple,1))
#     # print(target.iloc[:,21:26].apply(tuple,1))
#     # ambil data yang update dari changes
#     modified = changes[changes[['OrderID','RoleNo']].apply(tuple,1).isin(target[['OrderID','RoleNo']].apply(tuple,1))]
#     total_row_upd = len(modified)
#     text_upd = f'total row update : {total_row_upd}'
#     print(text_upd)
#     print(modified.iloc[:,0:6])

#     # ambil data yang new dari changes
#     inserted = changes[~changes[['OrderID','RoleNo']].apply(tuple,1).isin(target[['OrderID','RoleNo']].apply(tuple,1))]
#     total_row_ins = len(inserted)
#     text_ins = f'total row inserted : {total_row_ins}'
#     print(text_ins)
#     print(inserted.iloc[:,0:6])

#     if modified.empty:
#         inserted.to_sql('FactOrderBased', schema = 'dwhrscm_talend',con = conn_dwh_sqlserver, if_exists = 'append', index=False)
#         print('success insert all data without update')
    
#     else:
#         # buat fungsi untuk update data ke tabel target
#         def updated_to_sql(df, table_name, key_1,key_2):
#             list_col = []
#             table=table_name
#             pk_1 = key_1
#             pk_2 = key_2
#             temp_table = f'{table}_temporary_table'
#             for col in df.columns:
#                 if col == pk_1 or col == pk_2:
#                     continue
#                 list_col.append(f'r.{col} = t.{col}')
#             df.to_sql(temp_table,schema='dwhrscm_talend',con=conn_dwh_sqlserver, if_exists='replace',index=False)
#             update_stmt_1 = f'UPDATE r '
#             update_stmt_2 = f'SET '
#             update_stmt_3 = ", ".join(list_col)
#             update_stmt_4 = f' FROM dwhrscm_talend.{table} r '
#             update_stmt_5 = f'INNER JOIN (SELECT * FROM dwhrscm_talend.{temp_table}) as t ON r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} '
#             update_stmt_6 = f' WHERE r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} '
#             update_stmt_7 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_4 + update_stmt_5 + update_stmt_6 +";"
#             delete_stmt_1 = f'DROP TABLE dwhrscm_talend.{temp_table}'
#             print(update_stmt_6)
#             conn_dwh_sqlserver.execute(update_stmt_6)
#             conn_dwh_sqlserver.execute(delete_stmt_1)

#         try:
#             # update data
#             updated_to_sql(modified, 'FactOrderBased', 'OrderID','RoleNo')
#             inserted.to_sql('FactOrderBased', schema='dwhrscm_talend',con=conn_dwh_sqlserver, if_exists ='append',index=False)
#             print('success update and insert all data')
        
#         except Exception as e:
#             print(e)

#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)

text=f'scheduler tanggal : {date}'
print(text)

# conn_ehr.close()
# db_connection.close_connection(conn_ehr_live)
db_connection.close_connection(conn_ehr)
db_connection.close_connection(conn_dwh_sqlserver)
db_connection.close_connection(conn_his)