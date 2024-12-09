from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine
import pandas as pd
import time
import datetime as dt
date = dt.datetime.today()
import sys

sys.stdout = open("C:/TestPython/DWH_MySQL/logs/LogFactPatientDrugDispense.txt","w")

t0 = time.time()
ehr = create_engine('mysql://hanif-ppi:hanif2022@172.16.19.11/ehr')
dwh_talend = create_engine('mysql://hanif-ppi:hanif2022@172.16.5.33/dwhrscm_talend')

try:
    conn_ehr = ehr.connect()
    conn_dwh = dwh_talend.connect()
    print('successfully connect DB')
except SQLAlchemyError as e:
    error = str(e.__dict__['orig'])
    print(error)

source = pd.read_sql_query(""" 
                                SELECT
                                    a.prescription_id as PrescriptionID,
                                    e.order_id as OrderID,
                                    c.item_id as SequenceID,
                                    a.patient_id as PatientID, 
                                    a.admission_id as AdmissionID,
                                    b.patient_ext_id as MedicalNo,
                                    c.obj_id as DrugID, 
                                    d.obj_nm as DrugName,
                                    c.quantity_driver as DrugQuantity,
                                    CAST(c.unit_cost as decimal(30,4)) as DrugUnitPrice,
                                    CAST(c.tariff as decimal(30,4)) as DrugTotalPrice,
                                    c.unit_cd as UnitCode,  
                                    CASE 
                                        WHEN e.status_cd = 'nullified' THEN 'cancel'
                                        WHEN e.status_cd = 'normal' THEN 'final' 
                                    END as DispenseStatus,
                                    a.dispense_method as DispenseMethod,
                                    a.created_dttm as DrugPrescriptionDate,
                                    e.order_dttm as DrugDispenseDate,
                                    CASE
                                        WHEN f.obj_id is null then 0
                                        ELSE 1
                                    END AS IsFormulary
                                FROM xocp_ehr_prescription_x AS a
                                INNER JOIN xocp_ehr_ff_order e ON a.patient_id = e.patient_id and a.admission_id = e.admission_id and a.prescription_id = e.prescription_id
                                INNER join xocp_ehr_ff_item c ON a.prescription_id = c.prescription_id and a.patient_id = c.patient_id and a.admission_id = c.admission_id and e.order_id = c.order_id
                                INNER join xocp_ehr_obj d on c.obj_id = d.obj_id 
                                INNER JOIN xocp_ehr_patient b on a.patient_id = b.patient_id
                                LEFT JOIN xocp_inv_formulary f on c.obj_id = f.obj_id
                                WHERE a.admission_id <> '0' 
                                -- and a.created_dttm >= '2023-02-03 00:00:00' and a.created_dttm <= '2023-02-03 23:59:59'
                                -- and e.order_dttm >= '2023-03-06 00:00:00' AND e.order_dttm <= '2023-03-09 23:59:59' 
                                AND (e.order_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 4 DAY), "%%Y-%%m-%%d 00:00:00") AND e.order_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 23:59:59"))
                                and a.status_cd = 'final' 
                                AND b.patient_ext_id NOT IN ('','10-00','10-01','10-02','10-03','10-04','10-05','10-06','10-07','10-08','10-09','10-10', '410-40-67',
                                '345-01-01','igd304-11-22','igd304-12-12','igd345-10-10','IGD349-11-22','IGD11-22-44','igd654-32-21','igd-01-19',
                                'igd89-10-11','351-10-11','367-50-43','lat-ih-an','9-79-99','19-02-13','380-54-91',
                                '380-54-92','380-54-93','380-54-94','380-54-95',
                                '380-54-96','380-54-97','380-54-98','380-55-00','380-55-01',
                                '380-55-02','380-55-03','LAT-JT-01','380-55-05','380-55-06',
                                '380-55-07','380-55-08','380-55-09','380-55-10','380-55-11','380-55-12','380-55-13','380-55-04',
                                '380-55-58','380-55-65','latihanp-ri-nt','200000-00-00','a1','a2',
                                'a3','393-82-05','400-60-38','400-62-73','400-62-74',
                                '400-70-44','400-70-83','400-70-99','378-63-59',
                                '378-63-61','378-63-62','378-63-63','378-63-64','408-78-44','407-44-56',
                                '13-13-13','14-14-14','421-94-60','407-51-98',
                                '407-52-02','36800','44470','44471','44472','44473',
                                '44474','44475','44476','44478','44479','410-40-67','455-37-54','455-37-56','455-37-60',
                                '455-37-63','455-37-66','455-37-67','455-37-68','410-40-67'
                                ,'10-10', '410-40-67','10-11','10-12','10-13','10-14', '10-15','10-16','10-17','10-18','10-19','10-20','10-21','10-22','10-23','10-24','10-25',
                                '10-26','10-27','10-28','10-29','10-30','10-31','10-32','10-33','10-34','10-35','10-36',
                                '10-37','10-38','10-39','10-40',
                                '10-41','10-42','10-43','10-44','10-45','10-46','10-47','10-48','10-49','10-50')
                                order by a.prescription_id, e.order_id,c.item_id """, conn_ehr)
print(source)

# try:
#     # today = dt.datetime.now()
#     # today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
#     # source['InsertDateDWH'] = today_convert
#     source.to_sql('FactPatientDrugDispense', con=conn_dwh, if_exists = 'append', index=False)
#     print('success insert all data without update')
# except Exception as e:
#     print(e)

if source.empty:
    print('tidak ada data dari source')
else:
    # jika dari source cuma 1 row
    if len(source) == 1:        
        # ambil primary key dari source, ambil index ke 0
        prescriptionid = source["PrescriptionID"].values[0]
        orderid = source["OrderID"].values[0]
        sequenceid = source["SequenceID"].values[0]

        # query buat narik data dari target lalu filter berdasarkan primary key
        query = f'SELECT PrescriptionID,OrderID,SequenceID,PatientID,AdmissionID,MedicalNo,DrugID,DrugName,DrugQuantity, DrugUnitPrice,DrugTotalPrice,UnitCode,DispenseStatus,DispenseMethod,DrugPrescriptionDate,DrugDispenseDate,IsFormulary from FactPatientDrugDispense where PrescriptionID IN ({prescriptionid}) AND OrderID IN ({orderid}) AND SequenceID IN ({sequenceid}) order by PrescriptionID,OrderID,SequenceID'
        target = pd.read_sql_query(query, conn_dwh)
    else :
         # ambil primary key dari source, pake unique biar tidak duplicate
        prescriptionid = tuple(source["PrescriptionID"].unique())
        orderid = tuple(source["OrderID"].unique())
        sequenceid = tuple(source["SequenceID"].unique())

         # query buat narik data dari target lalu filter berdasarkan primary key
        query = f'SELECT PrescriptionID,OrderID,SequenceID,PatientID,AdmissionID,MedicalNo,DrugID,DrugName,DrugQuantity, DrugUnitPrice,DrugTotalPrice,UnitCode,DispenseStatus,DispenseMethod,DrugPrescriptionDate,DrugDispenseDate,IsFormulary from FactPatientDrugDispense where PrescriptionID IN {prescriptionid} AND OrderID IN {orderid} AND SequenceID IN {sequenceid} order by PrescriptionID,OrderID,SequenceID'
        target = pd.read_sql_query(query, conn_dwh)

    # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

    # ambil data yang update dari changes
    modified = changes[changes[['PrescriptionID','OrderID','SequenceID']].apply(tuple,1).isin(target[['PrescriptionID','OrderID','SequenceID']].apply(tuple,1))]
    total_row_upd = len(modified)
    text_upd = f'total row update : {total_row_upd}'
    print(text_upd)
    print(modified)

    # ambil data yang new dari changes
    inserted = changes[~changes[['PrescriptionID','OrderID','SequenceID']].apply(tuple,1).isin(target[['PrescriptionID','OrderID','SequenceID']].apply(tuple,1))]
    total_row_ins = len(inserted)
    text_ins = f'total row inserted : {total_row_ins}'
    print(text_ins)
    print(inserted)

    if modified.empty:
        # bikin tanggal sekarang buat kolom InsertDateDWH
        # today = dt.datetime.now()
        # today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
        # inserted['InsertDateDWH'] = today_convert
        inserted.to_sql('FactPatientDrugDispense', con=conn_dwh, if_exists = 'append', index=False)
        print('success insert all data without update')
    
    else:
        # buat fungsi untuk update data ke tabel target
        def updated_to_sql(df, table_name, key_1,key_2,key_3):
            list_col = []
            table=table_name
            pk_1 = key_1
            pk_2 = key_2
            pk_3 = key_3
            temp_table = f'{table}_temporary_table'
            for col in df.columns:
                if col == pk_1 or col == pk_2 or col == pk_3:
                    continue
                list_col.append(f'r.{col} = t.{col}')
            df.to_sql(temp_table,con=conn_dwh, if_exists='replace',index=False)
            update_stmt_1 = f'UPDATE {table} r '
            update_stmt_2 = f'INNER JOIN (SELECT * FROM {temp_table}) as t ON r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} AND r.{pk_3} = t.{pk_3} '
            update_stmt_3 = f'SET '
            update_stmt_4 = ", ".join(list_col)
            update_stmt_5 = f' WHERE r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} AND r.{pk_3} = t.{pk_3} '
            update_stmt_6 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_4 + update_stmt_5 +";"
            delete_stmt_1 = f'DROP TABLE {temp_table}'
            print(update_stmt_6)
            conn_dwh.execute(update_stmt_6)
            conn_dwh.execute(delete_stmt_1)

        try:
            # update data
            updated_to_sql(modified, 'FactPatientDrugDispense', 'PrescriptionID', 'OrderID', 'SequenceID')

            # insert data baru
            # today = dt.datetime.now()
            # today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
            # inserted['InsertDateDWH'] = today_convert
            inserted.to_sql('FactPatientDrugDispense', con=conn_dwh, if_exists ='append',index=False)
            print('success update and insert all data')
        
        except Exception as e:
            print(e)

#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)

text=f'scheduler tanggal : {date}'
print(text)

conn_dwh.close()
conn_ehr.close()
sys.stdout.close()

