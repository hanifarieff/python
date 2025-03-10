import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pyodbc
import pandas as pd
import time
import datetime as dt

sys.stdout = open("C:/TestPython/DWH_SQL_Server/DWH/logs/LogFactPatientDrugBasedBilling.txt","w")
t0 = time.time()

conn_ehr = db_connection.create_connection(db_connection.replika_ehr)
conn_dwh_sqlserver = db_connection.create_connection(db_connection.dwh_sqlserver)

source = pd.read_sql_query(""" WITH test AS (SELECT 
                                    z.billing_id BillingID,
                                    y.item_id BillingItemID,
                                    a.patient_id PatientID,
                                    a.admission_id AdmissionID,
                                    b.patient_ext_id as MedicalNo,
                                    f.admission_dttm as AdmissionDate,
                                    f.dr1 as DoctorID,
                                    g.person_nm as DoctorName,
                                    a.prescription_id PrescriptionID,
                                    e.order_id as OrderID,
                                    c.item_id as SequenceID,
                                    c.obj_id as DrugID,
                                    d.obj_nm as DrugName,
                                    y.tariff as BillingTariff,
                                    c.quantity_driver as DrugQuantity,
                                    c.unit_cd AS UnitCode,
                                    c.unit_cost as DrugUnitPrice,
                                    c.tariff as DrugTotalPrice,
                                    e.order_dttm as DrugDispenseDate,
                                    e.status_cd as DispenseStatus
                                from xocp_ehr_billing z 
                                INNER join xocp_ehr_billing_item y on z.billing_id = y.billing_id
                                INNER join xocp_ehr_prescription_x a on z.patient_id = a.patient_id and z.admission_id = a.admission_id
                                INNER join xocp_ehr_ff_order e ON z.patient_id = e.patient_id and z.admission_id = e.admission_id and a.prescription_id = e.prescription_id and z.billing_id = e.billing_id and y.order_id = e.order_id
                                INNER join xocp_ehr_ff_item c ON a.prescription_id = c.prescription_id and a.patient_id = c.patient_id and a.admission_id = c.admission_id and e.order_id = c.order_id
                                INNER JOIN xocp_ehr_patient b on a.patient_id = b.patient_id
                                INNER join xocp_ehr_obj d on c.obj_id = d.obj_id 
                                INNER JOIN xocp_ehr_patient_admission f on f.patient_id = z.patient_id and f.admission_id = z.admission_id
                                INNER JOIN xocp_hrm_employee g on f.dr1 = g.employee_id
                                where 
                                -- z.billing_id = '00200000000014' AND
                                -- z.patient_id = 1625447 and z.admission_id = 2
                                -- z.created_dttm>= '2023-01-02 00:00:00' and z.created_dttm<= '2023-01-02 23:59:59'
                                f.admission_dttm>= '2022-12-31 00:00:00' and f.admission_dttm<= '2022-12-31 23:59:59'
                                and a.status_cd = 'final' 
                                and y.golongan = 'obat'
                                ),
                                test2 AS (
                                SELECT 
                                    x.purchase_letter_id as PurchaseLetterID,
                                    x.DrugDispenseDate,
                                    x.delivered_dttm as DrugPurchaseDate,
                                    x.DrugID,
                                    x.unit_cost as DrugPurchaseUnitPrice
                                    FROM
                                    (
                                        SELECT 
                                            b.purchase_letter_id,
                                            a.delivered_dttm,
                                            d.DrugID, 
                                            c.unit_cost,
                                            d.DrugDispenseDate,
                                            DENSE_RANK() OVER (PARTITION BY d.BillingID, d.DrugID, d.DrugDispenseDate ORDER BY ABS(TIMESTAMPDIFF(SECOND, d.DrugDispenseDate, a.delivered_dttm))) AS Ranked
                                        FROM xocp_inv_proc_purchase_receive a
                                        INNER JOIN xocp_inv_proc_purchase_receive_item b on a.purchase_receive_id = b.purchase_receive_id
                                        INNER JOIN xocp_inv_proc_purchase_letter_item c on b.purchase_letter_id = c.purchase_letter_id and b.obj_id = c.obj_id
                                        INNER JOIN test d on b.obj_id = d.DrugID
                                        WHERE a.delivered_dttm <= d.DrugDispenseDate
                                    ) x
                                    WHERE x.Ranked = 1
                                    GROUP BY
                                    x.purchase_letter_id,
                                    x.DrugDispenseDate,
                                    x.delivered_dttm ,
                                    x.DrugID,
                                    x.unit_cost
                                )

                                SELECT 
                                    a.BillingID,
                                    a.BillingItemID,
                                    a.PatientID,
                                    a.AdmissionID,
                                    a.MedicalNo,
                                    a.AdmissionDate,
                                    a.DoctorID,
                                    a.DoctorName,
                                    a.PrescriptionID,
                                    a.OrderID,
                                    a.SequenceID,
                                    a.DrugDispenseDate,
                                    a.DispenseStatus,
                                    a.DrugID,
                                    a.DrugName,
                                    CAST(a.BillingTariff as decimal(15,4)) as BillingTariff ,
                                    a.DrugQuantity,
                                    a.UnitCode,
                                    CAST(a.DrugUnitPrice as decimal(15,4)) as DrugUnitPrice,
                                    CAST(a.DrugTotalPrice as decimal(15,4)) AS DrugTotalPrice,
                                    CAST(b.DrugPurchaseUnitPrice as decimal(15,4)) as DrugPurchaseUnitPrice,
                                    b.DrugPurchaseDate
                                FROM test a 
                                LEFT JOIN test2 b on a.DrugID = b.DrugID and a.DrugDispenseDate = b.DrugDispenseDate
                                ORDER BY a.BillingID, a.PrescriptionID, a.OrderID,a.SequenceID
                               	 """, conn_ehr)

print(source)

try:
    with conn_dwh_sqlserver.begin() as transaction:
        source.to_sql('FactPatientDrugBasedBilling',schema='dwhrscm_talend',con=conn_dwh_sqlserver, if_exists = 'append',index=False)
        print('success insert')
except Exception as e:
    print(e)
# if source.empty:
#     print('tidak ada data dari source')
# else:
#     # jika dari source cuma 1 row
#     if len(source) == 1:        
#         # ambil primary key dari source, ambil index ke 0
#         prescriptionid = source["PrescriptionID"].values[0]
#         orderid = source["OrderID"].values[0]
#         sequenceid = source["SequenceID"].values[0]

#         # query buat narik data dari target lalu filter berdasarkan primary key
#         query = f'SELECT TRIM(PrescriptionID) as PrescriptionID,OrderID,SequenceID,PatientID,AdmissionID,MedicalNo,DrugID,DrugName,DrugQuantity,CAST(DrugUnitPrice as decimal(30,4)) as DrugUnitPrice,CAST(DrugTotalPrice as decimal(30,4)) as DrugTotalPrice,UnitCode,DispenseStatus,DispenseMethod,DrugPrescriptionDate,DrugDispenseDate from staging_rscm.TransPatientDrugDispense where PrescriptionID IN ({prescriptionid}) AND OrderID IN ({orderid}) AND SequenceID IN ({sequenceid}) order by PrescriptionID,OrderID,SequenceID'
#         target = pd.read_sql_query(query, conn_staging_sqlserver)
#     else :
#          # ambil primary key dari source, pake unique biar tidak duplicate
#         prescriptionid = tuple(source["PrescriptionID"].unique())
#         orderid = tuple(source["OrderID"].unique())
#         sequenceid = tuple(source["SequenceID"].unique())

#          # query buat narik data dari target lalu filter berdasarkan primary key
#         query = f'SELECT TRIM(PrescriptionID) as PrescriptionID,OrderID,SequenceID,PatientID,AdmissionID,MedicalNo,DrugID,DrugName,DrugQuantity,CAST(DrugUnitPrice as decimal(30,4)) as DrugUnitPrice,CAST(DrugTotalPrice as decimal(30,4)) as DrugTotalPrice,UnitCode,DispenseStatus,DispenseMethod,DrugPrescriptionDate,DrugDispenseDate from staging_rscm.TransPatientDrugDispense where PrescriptionID IN {prescriptionid} AND OrderID IN {orderid} AND SequenceID IN {sequenceid} order by PrescriptionID,OrderID,SequenceID'
#         target = pd.read_sql_query(query, conn_staging_sqlserver)

#     # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
#     changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

#     # ambil data yang update dari changes
#     modified = changes[changes[['PrescriptionID','OrderID','SequenceID']].apply(tuple,1).isin(target[['PrescriptionID','OrderID','SequenceID']].apply(tuple,1))]
#     total_row_upd = len(modified)
#     text_upd = f'total row update : {total_row_upd}'
#     print(text_upd)
#     print(modified)

#     # ambil data yang new dari changes
#     inserted = changes[~changes[['PrescriptionID','OrderID','SequenceID']].apply(tuple,1).isin(target[['PrescriptionID','OrderID','SequenceID']].apply(tuple,1))]
#     total_row_ins = len(inserted)
#     text_ins = f'total row inserted : {total_row_ins}'
#     print(text_ins)
#     print(inserted)

    # if modified.empty:
    #     # bikin tanggal sekarang buat kolom InsertDateStaging
    #     today = dt.datetime.now()
    #     today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
    #     inserted['InsertDateStaging'] = today_convert
    #     inserted.to_sql('TransPatientDrugDispense', schema='staging_rscm', con = conn_staging_sqlserver, if_exists = 'append', index=False)
    #     print('success insert all data without update')
    
    # else:
    #     # buat fungsi untuk update data ke tabel target
    #     def updated_to_sql(df, table_name, key_1,key_2,key_3):
    #         list_col = []
    #         table=table_name
    #         pk_1 = key_1
    #         pk_2 = key_2
    #         pk_3 = key_3
    #         temp_table = f'{table}_temporary_table'
    #         for col in df.columns:
    #             if col == pk_1 or col == pk_2 or col == pk_3:
    #                 continue
    #             list_col.append(f'r.{col} = t.{col}')
    #         df.to_sql(temp_table,schema = 'staging_rscm',con=conn_staging_sqlserver, if_exists='replace',index=False)
    #         update_stmt_1 = f'UPDATE r '
    #         update_stmt_2 = f'SET '
    #         update_stmt_3 = ", ".join(list_col)
    #         update_stmt_8 = f' , r.UpdateDateStaging = CONVERT(DATETIME2(0), GETDATE(),120)'
    #         update_stmt_4 = f' FROM staging_rscm.{table} r '
    #         update_stmt_5 = f'INNER JOIN (SELECT * FROM staging_rscm.{temp_table}) as t ON r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} AND r.{pk_3} = t.{pk_3} '
    #         update_stmt_6 = f'WHERE r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} AND r.{pk_3} = t.{pk_3} '
    #         update_stmt_7 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_8 + update_stmt_4 + update_stmt_5 + update_stmt_6 +";"
    #         delete_stmt_1 = f'DROP TABLE staging_rscm.{temp_table}'
    #         print(update_stmt_7)
    #         conn_staging_sqlserver.execute(update_stmt_7)
    #         conn_staging_sqlserver.execute(delete_stmt_1)

    #     try:
    #         # update data
    #         updated_to_sql(modified, 'TransPatientDrugDispense', 'PrescriptionID', 'OrderID', 'SequenceID')

    #         # insert data baru
    #         today = dt.datetime.now()
    #         today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
    #         inserted['InsertDateStaging'] = today_convert
    #         inserted.to_sql('TransPatientDrugDispense', schema='staging_rscm', con=conn_staging_sqlserver, if_exists ='append',index=False)
    #         print('success update and insert all data')
        
    #     except Exception as e:
    #         print(e)

#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)

conn_ehr.close()
conn_dwh_sqlserver.close()