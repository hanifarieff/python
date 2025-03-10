import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pyodbc
import pandas as pd
import time
import datetime as dt
date = dt.datetime.today()

# bikin log ke file
sys.stdout = open("C:/TestPython/DWH_SQL_Server/Staging/logs/LogFactPatientDrugDispenseRetur.txt","w")
t0 = time.time()

# bikin koneksi ke db
conn_ehr = db_connection.create_connection(db_connection.replika_ehr)
conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)
conn_dwh_sqlserver = db_connection.create_connection(db_connection.dwh_sqlserver)

# dump = pd.read_sql_query(""" select 
#                             [Patient ID] AS PatientID,
#                             AdmissionID
#                         from staging_rscm.AntibiotikDump
#                         WHERE NO >= 3 AND NO <= 50 """,conn_staging_sqlserver)
# print(dump)
# PatientID = tuple(dump["PatientID"].unique())
# AdmissionID = tuple(dump["AdmissionID"].unique())
# def remove_comma(x):
#         if len(x) == 1:
#             return str(x).replace(',','')
#         else:
#             return x
    
# PatientID = remove_comma(PatientID)
# AdmissionID = remove_comma(AdmissionID)

query_source = f""" SELECT
		e.prescription_id as PrescriptionID,
		e.order_id as OrderID,
		c.item_id as SequenceID,
		e.patient_id as PatientID, 
		e.admission_id as AdmissionID,
		b.patient_ext_id as MedicalNo,
		c.obj_id as DrugID, 
		d.obj_nm as DrugName,
		c.quantity_driver as DrugQuantity,
		-CAST(c.unit_cost as decimal(30,4)) as DrugUnitPrice,
		-CAST(c.tariff as decimal(30,4)) as DrugTotalPrice,
		c.unit_cd as UnitCode,  
		CASE 
				WHEN e.status_cd = 'nullified' THEN 'cancel'
				WHEN e.status_cd = 'normal' THEN 'final' 
				ELSE 'cancel'
		END as DispenseStatus,
		'retur' AS DispenseMethod,
		CASE
				WHEN forn.OBAT IS NULL THEN 0
				ELSE 1
		END AS IsFornas,
		emp.employee_id AS CreatedID,
		0 AS DoctorID,
		NULL AS DrugPrescriptionDate,
		e.order_dttm as DrugDispenseDate
FROM xocp_ehr_ff_order e 
INNER join xocp_ehr_ff_item c ON e.prescription_id = c.prescription_id and e.patient_id = c.patient_id and e.admission_id = c.admission_id and e.order_id = c.order_id
INNER join xocp_ehr_obj d on c.obj_id = d.obj_id 
INNER JOIN xocp_ehr_patient b on e.patient_id = b.patient_id
LEFT JOIN FORNAS_BRANDS forn on c.obj_id = forn.obj_id
LEFT JOIN xocp_users usr on usr.user_id = e.created_user_id
LEFT JOIN xocp_hrm_employee emp on emp.person_id = usr.person_id
WHERE 
-- e.patient_id = 1896398 and e.admission_id = 1
e.order_dttm >= '2024-08-12 00:00:00' and e.order_dttm <= '2024-08-15 23:59:59'
AND e.return_ind = '1'
-- AND a.prescription_id = '00150000398652 '
ORDER BY e.prescription_id,e.order_id, c.item_id
"""
source = pd.read_sql_query(query_source,conn_ehr)

print(source)
source['DoctorID']=source['DoctorID'].astype('int64')
source['IsFornas']=source['IsFornas'].astype('int64')

if source.empty:
    print('tidak ada data dari source')
else:
    prescriptionid = tuple(source["PrescriptionID"].unique())
    orderid = tuple(source["OrderID"].unique())
    sequenceid = tuple(source["SequenceID"].unique())

    # bikin function remove comma jika ada variable yang isinya cuma 1, variable ini akan di pakai IN CLAUSE di query target
    def remove_comma(x):
        if len(x) == 1:
            return str(x).replace(',','')
        else:
            return x
    
    prescriptionid = remove_comma(prescriptionid)
    orderid = remove_comma(orderid)
    sequenceid = remove_comma(sequenceid)

    query = f'SELECT TRIM(PrescriptionID) as PrescriptionID,OrderID,SequenceID,PatientID,AdmissionID,MedicalNo,DrugID,DrugName,DrugQuantity,CAST(DrugUnitPrice as decimal(30,4)) as DrugUnitPrice,CAST(DrugTotalPrice as decimal(30,4)) as DrugTotalPrice,UnitCode,DispenseStatus,DispenseMethod,IsFornas,CreatedID,DoctorID,DrugPrescriptionDate,DrugDispenseDate from dwhrscm_talend.FactPatientDrugDispense where PrescriptionID IN {prescriptionid} AND OrderID IN {orderid} AND SequenceID IN {sequenceid} order by PrescriptionID,OrderID,SequenceID'
    target = pd.read_sql_query(query, conn_dwh_sqlserver)
    print(target)
    target['IsFornas'] = pd.to_numeric(target['IsFornas'], errors='coerce')
    target['IsFornas'].fillna(3,inplace=True)
    target['IsFornas'] = target['IsFornas'].astype('int64')

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
        if inserted.empty:
            print('there is no data updated and inserted')
        else:
            # bikin tanggal sekarang buat kolom InsertedDateDWH
            today = dt.datetime.now()
            today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
            inserted['InsertedDateDWH'] = today_convert
            inserted.to_sql('FactPatientDrugDispense', schema='dwhrscm_talend', con = conn_dwh_sqlserver, if_exists = 'append', index=False)
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
            df.to_sql(temp_table,schema = 'dwhrscm_talend',con=conn_dwh_sqlserver, if_exists='replace',index=False)
            update_stmt_1 = f'UPDATE r '
            update_stmt_2 = f'SET '
            update_stmt_3 = ", ".join(list_col)
            update_stmt_8 = f' , r.UpdateDateDWH = CONVERT(DATETIME2(0), GETDATE(),120)'
            update_stmt_4 = f' FROM dwhrscm_talend.{table} r '
            update_stmt_5 = f'INNER JOIN (SELECT * FROM dwhrscm_talend.{temp_table}) as t ON r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} AND r.{pk_3} = t.{pk_3} '
            update_stmt_6 = f'WHERE r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} AND r.{pk_3} = t.{pk_3} '
            update_stmt_7 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_8 + update_stmt_4 + update_stmt_5 + update_stmt_6 +";"
            delete_stmt_1 = f'DROP TABLE dwhrscm_talend.{temp_table}'
            print(update_stmt_7)
            conn_dwh_sqlserver.execute(update_stmt_7)
            conn_dwh_sqlserver.execute(delete_stmt_1)

        try:
            # update data
            updated_to_sql(modified, 'FactPatientDrugDispense', 'PrescriptionID', 'OrderID', 'SequenceID')

            # insert data baru
            today = dt.datetime.now()
            today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
            inserted['InsertedDateDWH'] = today_convert
            inserted.to_sql('FactPatientDrugDispense', schema='dwhrscm_talend', con=conn_dwh_sqlserver, if_exists ='append',index=False)
            print('success update and insert all data')
        
        except Exception as e:
            print(e)

#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)

text=f'scheduler tanggal : {date}'
print(text)

db_connection.close_connection(conn_ehr)
db_connection.close_connection(conn_staging_sqlserver)