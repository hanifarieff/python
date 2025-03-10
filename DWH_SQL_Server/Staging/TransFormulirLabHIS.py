import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pyodbc
import pandas as pd
import time
import datetime as dt
date = dt.datetime.today()

# bikin log ke file
sys.stdout = open("C:/TestPython/DWH_SQL_Server/Staging/logs/LogTransFormulirLabHIS.txt","w")
t0 = time.time()

conn_his_live = db_connection.create_connection(db_connection.his_live)
conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)

source = pd.read_sql_query("""
                           SELECT
                                a.patient_id as PatientID,
                                a.admission_id as AdmissionID,
                                a.form_number as FormNumber,
                                b.client_org_id as OrgID,
                                e.org_nm,
                                b.obj_id as ObjectID,
                                c.obj_nm as ObjectName,
                                d.admission_dttm as AdmissionDate,
                                b.created_dttm as CreatedDate,
                                b.created_user_id as CreatedBy,
                                b.status_cd as StatusCode,
                                a.lab_number as OrderLab,
                                b.order_id as OrderID,
                                a.patient_priority as PriorityPatient,
                                a.dpjp_employee_id as DoctorEmployeeID,
                                b.verified_user_id as VerifiedBy,
                                b.verified_dttm as VerifiedDate,
                                b.rejected_user_id as RejectedBy,
                                b.rejected_dttm as RejectedDate,
                                a.approved_user_id as ApprovedBy,
                                a.approved_dttm as ApprovedDate,
                                a.cancelled_user_id as CancelledBy,
                                a.cancelled_dttm as CancelledDate, 
                                a.dest_org_id as DstOrg,
                                NULL as ApprovalOrg,
                                TRIM(a.diag_txt) as Diagnose,
                                TRIM(b.notes) as AddedNotes,
                                TRIM(b.obj_ext) as Modality,
                                NULL as PatientStatus,
                                a.fast_cd as FastingStatus,
                                'no' as UrineStatus,
                                NULL as ThalasemiaStatus,
                                NULL as LiquidSamplingStatus,
                                NULL as OperationPreparationStatus,
                                b.order_no as OrderNo,
                                NULL as OrderIDRME,
                                NULL AS ScheduleID,
                                b.form_lab_detail_id as FormLabDetailID,
								f.loinc_id as LoincID
                            FROM xocp_his_form_lab a
                            LEFT JOIN xocp_his_form_lab_detail b ON a.form_number = b.form_number
                            LEFT JOIN xocp_obj c on b.obj_id = c.obj_id
                            LEFT JOIN xocp_his_patient_admission d on a.patient_id = d.patient_id and a.admission_id = d.admission_id
                            LEFT JOIN xocp_orgs e on a.client_org_id = e.org_id
							LEFT JOIN (SELECT obj_id_external, loinc_id
                                    FROM xocp_obj_mapping_ehr
                                    where (loinc_id IS NOT NULL AND loinc_id != '')
                                    AND variable_class_id = 6
                                    GROUP BY obj_id_external,loinc_id 
                            ) f on f.obj_id_external = TRIM(b.obj_ext)
                            -- WHERE a.status_cd = 'new'
                            WHERE
                            -- a.form_number IN (190692,191345)
                            -- a.created_dttm >= '2024-10-16 00:00:00' AND a.created_dttm <= '2024-10-20 23:59:59'
                            a.created_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 5 DAY), "%%Y-%%m-%%d 00:00:00") AND a.created_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 23:59:59")
                            -- a.created_dttm >= '2023-06-01 00:00:00' AND a.created_dttm <= '2023-06-15 23:59:59'
                            -- OR (cancelled_dttm >= '2022-11-01 00:00:00' AND cancelled_dttm <= '2022-11-31 23:59:59')
                            -- AND a.client_org_id in (select org_id from xocp_orgs where parent_id in ('687','1872','2418') or org_id in ('687','1872','2418'))
                            AND b.obj_id <> ' '
                            ORDER BY a.patient_id, a.admission_id,a.form_number, b.created_dttm asc

                         """,con=conn_his_live)
source.replace({pd.NaT: None},inplace=True)
source['Flag'] = 2
new_order_columns = ['PatientID','AdmissionID','FormNumber','OrgID','ObjectID','ObjectName','AdmissionDate','CreatedDate','CreatedBy',
                     'StatusCode','OrderLab','OrderID','PriorityPatient','DoctorEmployeeID','VerifiedBy','VerifiedDate','RejectedBy',
                     'RejectedDate','ApprovedBy','ApprovedDate','CancelledBy','CancelledDate','DstOrg','ApprovalOrg','Diagnose','AddedNotes',
                     'Modality','PatientStatus','FastingStatus','UrineStatus','ThalasemiaStatus','LiquidSamplingStatus','OperationPreparationStatus',
                     'OrderNo','OrderIDRME','ScheduleID','Flag','FormLabDetailID','LoincID']
source= source.reindex(columns=new_order_columns)
source['OrderID'] = source['OrderID'].fillna('')
source['CancelledBy'] = source['CancelledBy'].fillna(0)
source['ApprovalOrg'] = source['ApprovalOrg'].fillna(0)
source['ApprovedBy'] = source['ApprovedBy'].replace('',0)
source['ApprovedBy'] = source['ApprovedBy'].fillna(0)
source['OrderLab'] = source['OrderLab'].fillna(0).astype('int64')
source['OrderLab'] = source['OrderLab'].replace(0,'').astype('str')
source['DstOrg'] = source['DstOrg'].astype('int64')
source['DstOrg'] = source['DstOrg'].astype('int64')
source['ApprovedBy'] = source['ApprovedBy'].astype('int64')
source['CancelledBy'] = source['CancelledBy'].astype('int64')
print(source.iloc[:,:11])
# print(source.dtypes)
# cek = source[~(source["CancelledBy"] ==0)]
# print(cek)
# try:
#     today = dt.datetime.now()
#     today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
#     source['InsertDateStaging'] = today_convert
#     source.to_sql('TransFormulirLab', schema='staging_rscm', con = conn_staging_sqlserver, if_exists = 'append', index=False)
#     print('success insert all data without update')
# except Exception as e:
#     print(e)

if source.empty:
    print('tidak ada data dari source')
else:
    # jika dari source cuma 1 row
    if len(source) == 1:        
        # ambil primary key dari source, ambil index ke 0
        formnumber = source["FormNumber"].values[0]
        objid = source["ObjectID"].values[0]
        orderid = source["OrderID"].values[0]
        orderid = orderid + (' ',)
        orderno = source["OrderNo"].values[0]
        formlabdetailid = source["FormLabDetailID"].values[0]

        # query buat narik data dari target lalu filter berdasarkan primary key
        query = f"SELECT PatientID,AdmissionID,FormNumber,OrgID, TRIM(ObjectID) as ObjectID, ObjectName, AdmissionDate,CreatedDate,CreatedBy,StatusCode,TRIM(OrderLab) as OrderLab,TRIM(OrderID) AS OrderID,PriorityPatient,DoctorEmployeeID, VerifiedBy,VerifiedDate,RejectedBy,RejectedDate,ApprovedBy,ApprovedDate,CancelledBy,CancelledDate,DstOrg,ApprovalOrg,TRIM(Diagnose) as Diagnose,TRIM(AddedNotes) as AddedNotes,TRIM(Modality) as Modality,PatientStatus,FastingStatus,UrineStatus, ThalasemiaStatus, LiquidSamplingStatus,OperationPreparationStatus, TRIM(OrderNo) as OrderNo,OrderIDRME, ScheduleID, Flag, FormLabDetailID,LoincID from staging_rscm.TransFormulirLab where FormNumber IN ({formnumber}) AND ObjectID IN({objid}) AND OrderID IN ({orderid}) AND OrderNo IN ({orderno}) AND FormLabDetailID IN ({formlabdetailid}) AND Flag = 2 order by PatientID,AdmissionID, FormNumber, CreatedDate ASC"
        target = pd.read_sql_query(query, conn_staging_sqlserver)
        
    else :
         # ambil primary key dari source, pake unique biar tidak duplicate
        formnumber = tuple(source["FormNumber"])
        objid = tuple(source["ObjectID"].unique())
        orderid = tuple(source["OrderID"].unique())
        orderid = orderid + (' ',)
        orderno = tuple(source["OrderNo"])
        formlabdetailid = tuple(source["FormLabDetailID"].unique())

         # query buat narik data dari target lalu filter berdasarkan primary key
        query = f"SELECT PatientID,AdmissionID,FormNumber,OrgID, TRIM(ObjectID) as ObjectID, ObjectName,AdmissionDate,CreatedDate,CreatedBy,StatusCode,TRIM(OrderLab) as OrderLab,TRIM(OrderID) AS OrderID,PriorityPatient,DoctorEmployeeID, VerifiedBy,VerifiedDate,RejectedBy,RejectedDate,ApprovedBy,ApprovedDate,CancelledBy,CancelledDate,DstOrg,ApprovalOrg,TRIM(Diagnose) as Diagnose,TRIM(AddedNotes) as AddedNotes,TRIM(Modality) as Modality,PatientStatus,FastingStatus,UrineStatus, ThalasemiaStatus, LiquidSamplingStatus,OperationPreparationStatus, TRIM(OrderNo) as OrderNo, OrderIDRME, ScheduleID, Flag, FormLabDetailID,LoincID from staging_rscm.TransFormulirLab where FormNumber IN {formnumber} AND ObjectID IN {objid} AND OrderID IN {orderid} AND OrderNo IN {orderno} AND FormLabDetailID IN {formlabdetailid} AND Flag = 2 order by PatientID,AdmissionID, FormNumber, CreatedDate ASC"        
        target = pd.read_sql_query(query, conn_staging_sqlserver)

        # karena ada beberapa tanggal yang kosong, ubah format tanggal NaT menjadi None
        target.replace({pd.NaT: None},inplace=True)
    # print(source.dtypes)
    # print(target.dtypes)
    # # print(target.dtypes)
    # print(source.iloc[:,0:3].apply(tuple,1))
    # print(target.iloc[:,0:3].apply(tuple,1))
    # print(source.iloc[:,2:4].apply(tuple,1))
    # print(target.iloc[:,2:4].apply(tuple,1))
    # print(source.iloc[:,3:5].apply(tuple,1))
    # print(target.iloc[:,3:5].apply(tuple,1))
    # print(source.iloc[:,4:6].apply(tuple,1))
    # print(target.iloc[:,4:6].apply(tuple,1))
    # print(source.iloc[:,5:7].apply(tuple,1))
    # print(target.iloc[:,5:7].apply(tuple,1))
    # print('bates')
    # print(source.iloc[:,7:9].apply(tuple,1))
    # print(target.iloc[:,7:9].apply(tuple,1))
    # print(source.iloc[:,9:11].apply(tuple,1))
    # print(target.iloc[:,9:11].apply(tuple,1))
    # print(source.iloc[:,11:13].apply(tuple,1))
    # print(target.iloc[:,11:13].apply(tuple,1))
    # print(source.iloc[:,13:15].apply(tuple,1))
    # print(target.iloc[:,13:15].apply(tuple,1))
    # print(source.iloc[:,15:17].apply(tuple,1))
    # print(target.iloc[:,15:17].apply(tuple,1))
    # print(source.iloc[:,17:20].apply(tuple,1))
    # print(target.iloc[:,17:20].apply(tuple,1))
    # print(source.iloc[:,20:24].apply(tuple,1))
    # print(target.iloc[:,20:24].apply(tuple,1))
    # print('bates lagi')
    # print(source.iloc[:,21:25].apply(tuple,1))
    # print(target.iloc[:,21:25].apply(tuple,1))
    # print(source.iloc[:,25:31].apply(tuple,1))
    # print(target.iloc[:,25:31].apply(tuple,1))
    # print(source.iloc[:,31:36].apply(tuple,1))
    # print(target.iloc[:,31:36].apply(tuple,1))
    # print(source.iloc[:,36:39].apply(tuple,1))
    # print(target.iloc[:,36:39].apply(tuple,1))
    # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]
    print('ini changes')
    print(changes)
    print('\n')

    # ambil data yang update dari changes
    targetexceptnew = target[target['StatusCode'] != 'new']
    modifiedexceptnew = changes[changes[['FormNumber','ObjectID','OrderID','OrderNo','FormLabDetailID']].apply(tuple,1).isin(targetexceptnew[['FormNumber','ObjectID','OrderID','OrderNo','FormLabDetailID']].apply(tuple,1))]
    total_row_upd = len(modifiedexceptnew)
    text_upd = f'total row update except new : {total_row_upd}'
    print(text_upd)
    print(modifiedexceptnew)
    print('\n')

    targetnew = target[target['StatusCode'] == 'new']
    modifiednew = changes[changes[['FormNumber','ObjectID','OrderNo','FormLabDetailID']].apply(tuple,1).isin(targetnew[['FormNumber','ObjectID','OrderNo','FormLabDetailID']].apply(tuple,1))]
    total_row_upd = len(modifiednew)
    text_upd = f'total row update new : {total_row_upd}'
    print(text_upd)
    print(modifiednew)
    print('\n')

    # ambil data yang new dari changes
    inserted = changes[~changes[['FormNumber','ObjectID','OrderNo','FormLabDetailID']].apply(tuple,1).isin(target[['FormNumber','ObjectID','OrderNo','FormLabDetailID']].apply(tuple,1))]
    total_row_ins = len(inserted)
    text_ins = f'total row inserted : {total_row_ins}'
    print(text_ins)
    print(inserted)
    print('\n')

    if modifiedexceptnew.empty & modifiednew.empty & inserted.empty:
        print('tidak ada data yang bisa diproses karena data dari source kosong')

    elif modifiedexceptnew.empty & modifiednew.empty:
        # bikin tanggal sekarang buat kolom InsertDateStaging
        today = dt.datetime.now()
        today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
        inserted['InsertDateStaging'] = today_convert
        inserted.to_sql('TransFormulirLab', schema='staging_rscm', con = conn_staging_sqlserver, if_exists = 'append', index=False)
        print('success insert all data without update')
    
    else:
        #buat fungsi untuk update data ke tabel target
        def updated_to_except_new(df, table_name, key_1,key_2,key_3,key_4,key_5,key_6,key_7):
            list_col = []
            table=table_name
            pk_1 = key_1
            pk_2 = key_2
            pk_3 = key_3
            pk_4 = key_4
            pk_5 = key_5
            pk_6 = key_6
            pk_7 = key_7
            temp_table = f'{table}_temporary_table'
            for col in df.columns:
                if col == pk_1 or col == pk_2 or col == pk_3 or col == pk_4 or col == pk_5 or col == pk_6 or col == pk_7:
                    continue
                list_col.append(f'r.{col} = t.{col}')
            df.to_sql(temp_table,schema = 'staging_rscm',con=conn_staging_sqlserver, if_exists='replace',index=False)
            update_stmt_1 = f'UPDATE r '
            update_stmt_2 = f'SET '
            update_stmt_3 = ", ".join(list_col)
            update_stmt_8 = f' , r.UpdateDateStaging = CONVERT(DATETIME2(0), GETDATE(),120)'
            update_stmt_4 = f' FROM staging_rscm.{table} r '
            update_stmt_5 = f'INNER JOIN (SELECT * FROM staging_rscm.{temp_table}) as t ON r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} AND r.{pk_3} = t.{pk_3} AND r.{pk_4} = t.{pk_4} AND r.{pk_5} = t.{pk_5} AND r.{pk_6} = t.{pk_6} AND r.{pk_7} = t.{pk_7} '
            update_stmt_6 = f'WHERE r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} AND r.{pk_3} = t.{pk_3} AND r.{pk_4} = t.{pk_4} AND r.{pk_5} = t.{pk_5} AND r.{pk_6} = t.{pk_6} AND r.{pk_7} = t.{pk_7} '
            update_stmt_7 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_8 + update_stmt_4 + update_stmt_5 + update_stmt_6 +";"
            delete_stmt_1 = f'DROP TABLE staging_rscm.{temp_table}'
            print(update_stmt_7)
            print(delete_stmt_1)
            conn_staging_sqlserver.execute(update_stmt_7)
            conn_staging_sqlserver.execute(delete_stmt_1)

        def updated_to_new(df, table_name, key_1,key_2,key_3,key_4,key_5,key_6):
            list_col = []
            table=table_name
            pk_1 = key_1
            pk_2 = key_2
            pk_3 = key_3
            pk_4 = key_4
            pk_5 = key_5
            pk_6 = key_6
            temp_table = f'{table}_temporary_table'
            for col in df.columns:
                if col == pk_1 or col == pk_2 or col == pk_3 or col == pk_4 or col == pk_5 or col == pk_6 :
                    continue
                list_col.append(f'r.{col} = t.{col}')
            df.to_sql(temp_table,schema = 'staging_rscm',con=conn_staging_sqlserver, if_exists='replace',index=False)
            update_stmt_1 = f'UPDATE r '
            update_stmt_2 = f'SET '
            update_stmt_3 = ", ".join(list_col)
            update_stmt_8 = f' , r.UpdateDateStaging = CONVERT(DATETIME2(0), GETDATE(),120)'
            update_stmt_4 = f' FROM staging_rscm.{table} r '
            update_stmt_5 = f'INNER JOIN (SELECT * FROM staging_rscm.{temp_table}) as t ON r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} AND r.{pk_3} = t.{pk_3} AND r.{pk_4} = t.{pk_4} AND r.{pk_5} = t.{pk_5} AND r.{pk_6} = t.{pk_6} '
            update_stmt_6 = f'WHERE r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} AND r.{pk_3} = t.{pk_3} AND r.{pk_4} = t.{pk_4} AND r.{pk_5} = t.{pk_5} AND r.{pk_6} = t.{pk_6} '
            update_stmt_7 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_8 + update_stmt_4 + update_stmt_5 + update_stmt_6 +";"
            delete_stmt_1 = f'DROP TABLE staging_rscm.{temp_table}'
            print(update_stmt_7)
            print(delete_stmt_1)
            conn_staging_sqlserver.execute(update_stmt_7)
            conn_staging_sqlserver.execute(delete_stmt_1)
            print('\n')

        try:
            # update data yang status codenya new
            updated_to_new(modifiednew, 'TransFormulirLab', 'PatientID','AdmissionID','FormNumber', 'ObjectID','OrderNo','FormLabDetailID')
     
            # update data yang status codenya selain new
            updated_to_except_new(modifiedexceptnew, 'TransFormulirLab',  'PatientID','AdmissionID','FormNumber','ObjectID', 'OrderID','OrderNo','FormLabDetailID')
            # insert data baru
            today = dt.datetime.now()
            today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
            inserted['InsertDateStaging'] = today_convert
            inserted.to_sql('TransFormulirLab', schema='staging_rscm', con=conn_staging_sqlserver, if_exists ='append',index=False)
            print('success update and insert all data')
        except Exception as e:
            print(e)


#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)

text=f'scheduler tanggal : {date}'
print(text)

db_connection.close_connection(conn_his_live)
db_connection.close_connection(conn_staging_sqlserver)