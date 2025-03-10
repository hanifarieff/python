import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pyodbc
import pandas as pd
import time
import datetime as dt
date = dt.datetime.today()

# bikin log ke file
sys.stdout = open("C:/TestPython/DWH_SQL_Server/Staging/logs/LogTransPatientDrugDispense.txt","w")
t0 = time.time()

# bikin koneksi ke db
conn_ehr = db_connection.create_connection(db_connection.replika_ehr)
conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)

source = pd.read_sql_query(""" SELECT
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
                                        ELSE 'cancel'
                                    END as DispenseStatus,
                                    a.dispense_method as DispenseMethod,
                                    CASE
                                        WHEN forn.OBAT IS NULL THEN 0
                                        ELSE 1
                                    END AS IsFornas,
                           		    emp.employee_id as CreatedID,
		                            a.dr_id as DoctorID,
                                    a.created_dttm as DrugPrescriptionDate,
                                    e.order_dttm as DrugDispenseDate,
                                    e.billing_paid as BillingPaid
                                FROM xocp_ehr_prescription_x AS a
                                INNER JOIN xocp_ehr_ff_order e ON a.patient_id = e.patient_id and a.admission_id = e.admission_id and a.prescription_id = e.prescription_id
                                INNER join xocp_ehr_ff_item c ON a.prescription_id = c.prescription_id and a.patient_id = c.patient_id and a.admission_id = c.admission_id and e.order_id = c.order_id
                                INNER join xocp_ehr_obj d on c.obj_id = d.obj_id 
                                INNER JOIN xocp_ehr_patient b on a.patient_id = b.patient_id
                                LEFT JOIN FORNAS_BRANDS forn on c.obj_id = forn.obj_id
                                LEFT JOIN xocp_users usr on usr.user_id = a.created_user_id
                                LEFT JOIN xocp_hrm_employee emp on emp.person_id = usr.person_id
                                WHERE a.admission_id <> '0' 
                                -- AND a.prescription_id = '00150000398652 '
                                -- and a.patient_id = 1732667 AND a.admission_id = 19
                                -- AND a.prescription_id IN ('00140002437059','00150000153444','00150000208564','00150000011074','00150000123010','00150000152310','00150000040939','00150000026925','00150000062326','00150000219091','00150000076421','00150000164458','00150000146798','00150000065176','00150000205798','00150000208581','00150000081210','00150000074599','00150000207898','00150000025670','00150000075502','00150000016857','00140002487578','00150000057612','00150000237334','00140002236451','00150000205297','00150000173003','00150000178800','00150000187220','00150000091964','00150000093475','00150000086102','00150000208535','00150000012320','00150000128668','00140002463109','00150000096403','00150000201509','00150000044557','00150000081566','00150000166435','00150000016680','00150000016833','00150000117431','00150000208710','00150000076559','00150000209257','00150000176834','00140002418089','00150000153553','00150000026723','00150000084581','00150000186616','00150000069386','00150000153422','00140002217163','00150000220301','00150000076638','00140002451240','00150000184399','00140002302592','00150000027333','00150000177265','00150000129641','00150000041601','00150000044381','00150000143635','00150000018097','00150000039002','00150000206305','00150000036615','00150000039003','00150000114324','00150000052473','00150000075601','00140002427979','00150000099312','00150000183157','00150000216176','00150000153434','00150000177196','00150000237018','00150000094392','00150000055171','00150000133790','00150000075717','00150000226337','00150000075496','00150000043656','00150000163366','00150000183007','00150000095114','00150000129588','00150000208762','00150000044156','00150000189589','00150000077073','00150000105953','00150000076575','00150000129293','00150000098872','00150000216178','00150000163370','00150000121564','00150000230080','00150000026024','00150000177169','00150000214118','00150000012385','00150000208533','00150000165805','00150000153417','00150000153765','00150000147876','00140002538735','00150000012625','00140002423966','00150000146586','00150000208482','00150000164502','00150000044775','00150000057565','00150000019290','00150000059889','00150000069085','00150000215242','00150000051918','00150000206236','00150000208563','00150000043426','00150000035188','00150000067007','00150000076978','00150000230742','00150000230559','00150000221791','00150000208604','00150000084570','00150000082218','00150000170219','00150000066312','00150000181325','00150000208484','00150000042106','00150000098916','00150000183054','00150000135062','00150000164755','00150000193512','00150000052663','00150000043882','00150000074178','00150000034072','00140002520812','00150000174625','00140002406916','00150000074017','00150000075285','00150000025583','00150000065717','00150000091771','00150000182212','00150000129634','00140002553441','00150000153407','00150000042518','00150000164772','00150000043483','00150000185279','00150000122355','00150000125742','00150000043641','00150000097155','00150000127410','00150000208524','00150000091796','00140002503232','00150000176588','00150000179590','00150000026019','00150000203803','00150000177317','00150000208688','00140002185661','00150000065781','00140002459763','00150000043840','00150000166489','00150000129126','00150000072935','00150000039007','00150000172645','00150000042372','00150000098888','00150000177581','00150000209271','00140002552699','00150000088723','00150000057643','00150000037351','00150000094252','00150000230532','00150000147019','00150000043374','00150000119518','00150000071611','00140002463722','00150000147894','00150000084736','00150000066181','00150000153551','00150000147092','00150000095270','00150000025380','00150000039039','00150000208543','00150000146355','00150000044608','00150000221693','00150000012378','00150000044456','00150000097051','00150000153402','00150000111193','00150000148877','00150000080915','00150000208536','00150000128289','00150000111360','00150000150841','00150000143331','00150000193189','00150000208574','00150000208542','00150000142741','00150000235174','00150000020191','00150000153591','00150000017994','00150000083148','00150000125827','00150000226269','00150000087990','00150000208613','00150000230606','00150000193396','00150000208752')
                                -- and e.order_dttm >= '2024-07-05 00:00:00' and e.order_dttm <= '2024-07-06 23:59:59'
                                AND (e.order_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 2 DAY), "%%Y-%%m-%%d 00:00:00") AND e.order_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 23:59:59")) 
                                and a.status_cd = 'final' 
                                order by a.prescription_id, e.order_id,c.item_id """, conn_ehr)

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

    query = f'SELECT TRIM(PrescriptionID) as PrescriptionID,OrderID,SequenceID,PatientID,AdmissionID,MedicalNo,DrugID,DrugName,DrugQuantity,CAST(DrugUnitPrice as decimal(30,4)) as DrugUnitPrice,CAST(DrugTotalPrice as decimal(30,4)) as DrugTotalPrice,UnitCode,DispenseStatus,DispenseMethod,IsFornas,CreatedID,DoctorID,DrugPrescriptionDate,DrugDispenseDate,BillingPaid from staging_rscm.TransPatientDrugDispense where PrescriptionID IN {prescriptionid} AND OrderID IN {orderid} AND SequenceID IN {sequenceid} order by PrescriptionID,OrderID,SequenceID'
    target = pd.read_sql_query(query, conn_staging_sqlserver)
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
            # bikin tanggal sekarang buat kolom InsertDateStaging
            today = dt.datetime.now()
            today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
            inserted['InsertDateStaging'] = today_convert
            inserted.to_sql('TransPatientDrugDispense', schema='staging_rscm', con = conn_staging_sqlserver, if_exists = 'append', index=False)
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
            df.to_sql(temp_table,schema = 'staging_rscm',con=conn_staging_sqlserver, if_exists='replace',index=False)
            update_stmt_1 = f'UPDATE r '
            update_stmt_2 = f'SET '
            update_stmt_3 = ", ".join(list_col)
            update_stmt_8 = f' , r.UpdateDateStaging = CONVERT(DATETIME2(0), GETDATE(),120)'
            update_stmt_4 = f' FROM staging_rscm.{table} r '
            update_stmt_5 = f'INNER JOIN (SELECT * FROM staging_rscm.{temp_table}) as t ON r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} AND r.{pk_3} = t.{pk_3} '
            update_stmt_6 = f'WHERE r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} AND r.{pk_3} = t.{pk_3} '
            update_stmt_7 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_8 + update_stmt_4 + update_stmt_5 + update_stmt_6 +";"
            delete_stmt_1 = f'DROP TABLE staging_rscm.{temp_table}'
            print(update_stmt_7)
            conn_staging_sqlserver.execute(update_stmt_7)
            conn_staging_sqlserver.execute(delete_stmt_1)

        try:
            # update data
            updated_to_sql(modified, 'TransPatientDrugDispense', 'PrescriptionID', 'OrderID', 'SequenceID')

            # insert data baru
            today = dt.datetime.now()
            today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
            inserted['InsertDateStaging'] = today_convert
            inserted.to_sql('TransPatientDrugDispense', schema='staging_rscm', con=conn_staging_sqlserver, if_exists ='append',index=False)
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