import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pyodbc
import pandas as pd
import time
import datetime as dt
import numpy as np
date = dt.datetime.today()

# bikin log ke file
sys.stdout = open("C:/TestPython/DWH_SQL_Server/Staging/logs/LogTransFormulirLabEHR.txt","w")
t0 = time.time()

conn_ehr = db_connection.create_connection(db_connection.replika_ehr)
conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)
conn_his_live = db_connection.create_connection(db_connection.his_live)
source = pd.read_sql_query("""
                            SELECT
                                    a.patient_id as PatientID,
                                    a.admission_id as AdmissionID,
                                    a.form_number as FormNumber,
                                    a.org_id as OrgID,
                                    a.obj_id as ObjectID,
                                    b.obj_nm as ObjectName,
                                    c.admission_dttm as AdmissionDate,
                                    a.created_dttm as CreatedDate,
                                    a.created_by as CreatedBy,
                                    a.status_cd as StatusCode,
                                    a.order_lab as OrderLab,
                                    a.order_id as OrderID,
                                    a.prioritas_pasien as PriorityPatient,
                                    a.dr_employeeid as DoctorEmployeeID,
                                    a.verified_by as VerifiedBy,
                                    a.verified_dttm as VerifiedDate,
                                    a.rejected_by as RejectedBy,
                                    a.rejected_dttm as RejectedDate,
                                    a.approved_by as ApprovedBy,
                                    a.approved_dttm as ApprovedDate,
                                    a.cancelled_by as CancelledBy,
                                    a.cancelled_dttm as CancelledDate, 
                                    -- cancelled_note as CancelledNote,
                                    -- desc_obj_id as DescObjectID,
                                    a.dst_org as DstOrg,
                                    a.approval_org as ApprovalOrg,
                                    TRIM(a.dx) as Diagnose,
                                    TRIM(a.catatan_tambahan) as AddedNotes,
                                    TRIM(a.modality) as Modality,
                                    a.status_pasien as PatientStatus,
                                    a.status_puasa as FastingStatus,
                                    a.status_urine as UrineStatus,
                                    a.status_thalasemia as ThalasemiaStatus,
                                    a.status_sampling_cairan as LiquidSamplingStatus,
                                    a.status_persiapan_operasi as OperationPreparationStatus,
                                    a.order_no as OrderNo,
                                    a.order_id_rme as OrderIDRME,
                                    a.schedule_id as ScheduleID,
                                    0 as FormLabDetailID
                            FROM xocp_ehr_formulir_lab a
                            INNER JOIN xocp_ehr_obj b ON a.obj_id = b.obj_id
                            INNER JOIN xocp_ehr_patient_admission c on a.patient_id = c.patient_id and a.admission_id = c.admission_id
                            -- WHERE a.status_cd = 'new'
                            WHERE
                            -- a.form_number IN (411823222)
                           -- AND
                            -- a.created_dttm >= '2024-05-11 00:00:00' AND a.created_dttm <= '2024-05-11 23:59:59'
                            (a.created_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 00:00:00") AND a.created_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 23:59:59")) 
                            OR
                            (a.updated_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 00:00:00") AND a.updated_dttm <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d 23:59:59")) 
                            -- AND a.org_id in (select org_id from xocp_orgs where parent_id not in ('687','1872','2418') and org_id not in ('687','1872','2418'))
                            AND a.obj_id <> ' ' AND a.form_number_his = 0
                            ORDER BY a.patient_id, a.admission_id, a.order_no 


                         """,con=conn_ehr)
source.replace({pd.NaT: None},inplace=True)
source['Flag'] = 1
new_order_columns = ['PatientID','AdmissionID','FormNumber','OrgID','ObjectID','ObjectName','AdmissionDate','CreatedDate','CreatedBy',
                     'StatusCode','OrderLab','OrderID','PriorityPatient','DoctorEmployeeID','VerifiedBy','VerifiedDate','RejectedBy',
                     'RejectedDate','ApprovedBy','ApprovedDate','CancelledBy','CancelledDate','DstOrg','ApprovalOrg','Diagnose','AddedNotes',
                     'Modality','PatientStatus','FastingStatus','UrineStatus','ThalasemiaStatus','LiquidSamplingStatus','OperationPreparationStatus',
                     'OrderNo','OrderIDRME','ScheduleID','Flag','FormLabDetailID']
source= source.reindex(columns=new_order_columns)
print(source.iloc[:,5:8])


# filter yang tanggal admisinya bukan null
source=source[source['AdmissionDate'].notna()]

# bikin query ambil loinc
query_loinc = f"""
                SELECT obj_id_external AS Modality, loinc_id as LoincID
                FROM xocp_obj_mapping_ehr
                where (loinc_id IS NOT NULL AND loinc_id != '')
                AND variable_class_id = 6
                GROUP BY obj_id_external,loinc_id 
            """
get_loinc = pd.read_sql_query(query_loinc, conn_his_live)
source = source.merge(get_loinc,how='left',on='Modality')
source.replace({np.nan: None},inplace=True)
print(source)
print(source.dtypes)


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

        # query buat narik data dari target lalu filter berdasarkan primary key
        query = f"SELECT PatientID,AdmissionID,FormNumber,OrgID, TRIM(ObjectID) as ObjectID,ObjectName, AdmissionDate, CreatedDate,CreatedBy,StatusCode,TRIM(OrderLab) as OrderLab,TRIM(OrderID) AS OrderID,PriorityPatient,DoctorEmployeeID, VerifiedBy,VerifiedDate,RejectedBy,RejectedDate,ApprovedBy,ApprovedDate,CancelledBy,CancelledDate,DstOrg,ApprovalOrg,TRIM(Diagnose) as Diagnose,TRIM(AddedNotes) as AddedNotes,TRIM(Modality) as Modality,PatientStatus,FastingStatus,UrineStatus,ThalasemiaStatus,LiquidSamplingStatus,OperationPreparationStatus,TRIM(OrderNo) as OrderNo,OrderIDRME,ScheduleID,Flag,FormLabDetailID, LoincID from staging_rscm.TransFormulirLab where FormNumber IN ({formnumber}) AND ObjectID IN({objid}) AND OrderID IN ({orderid}) AND OrderNo IN ({orderno}) AND Flag = 1 order by PatientID,AdmissionID, OrderNo"
        target = pd.read_sql_query(query, conn_staging_sqlserver)
        
    else :
         # ambil primary key dari source, pake unique biar tidak duplicate
        formnumber = tuple(source["FormNumber"])
        objid = tuple(source["ObjectID"].unique())
        orderid = tuple(source["OrderID"].unique())
        orderid = orderid + (' ',)
        orderno = tuple(source["OrderNo"].unique())

         # query buat narik data dari target lalu filter berdasarkan primary key
        query = f"SELECT PatientID,AdmissionID,FormNumber,OrgID, TRIM(ObjectID) as ObjectID,ObjectName, AdmissionDate, CreatedDate,CreatedBy,StatusCode,TRIM(OrderLab) as OrderLab,TRIM(OrderID) AS OrderID,PriorityPatient,DoctorEmployeeID, VerifiedBy,VerifiedDate,RejectedBy,RejectedDate,ApprovedBy,ApprovedDate,CancelledBy,CancelledDate,DstOrg,ApprovalOrg,TRIM(Diagnose) as Diagnose,TRIM(AddedNotes) as AddedNotes,TRIM(Modality) as Modality,PatientStatus,FastingStatus,UrineStatus,ThalasemiaStatus,LiquidSamplingStatus,OperationPreparationStatus,TRIM(OrderNo) as OrderNo,OrderIDRME,ScheduleID,Flag,FormLabDetailID,LoincID from staging_rscm.TransFormulirLab where FormNumber IN {formnumber} AND ObjectID IN {objid} AND OrderID IN {orderid} AND OrderNo IN {orderno} AND Flag = 1 order by PatientID,AdmissionID,  OrderNo"        
        target = pd.read_sql_query(query, conn_staging_sqlserver)

        # karena ada beberapa tanggal yang kosong, ubah format tanggal NaT menjadi None
        target.replace({pd.NaT: None},inplace=True)
    # print(target.dtypes)
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
    # # print(source.iloc[:,17:20].apply(tuple,1))
    # # print(target.iloc[:,17:20].apply(tuple,1))
    # # print(source.iloc[:,20:24].apply(tuple,1))
    # # print(target.iloc[:,20:24].apply(tuple,1))
    # print('bates lagi')
    # print(source.iloc[:,21:25].apply(tuple,1))
    # print(target.iloc[:,21:25].apply(tuple,1))
    # print(source.iloc[:,25:31].apply(tuple,1))
    # print(target.iloc[:,25:31].apply(tuple,1))
    # print(source.loc[:,['LoincID']].apply(tuple,1))
    # print(target.loc[:,['LoincID']].apply(tuple,1))

    # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

    print('ini changes')
    print(changes)
    print('\n')

    # ambil data yang update dari changes
    targetexceptnew = target[target['StatusCode'] != 'new']
    modifiedexceptnew = changes[changes[['FormNumber','ObjectID','OrderID','OrderNo']].apply(tuple,1).isin(targetexceptnew[['FormNumber','ObjectID','OrderID','OrderNo']].apply(tuple,1))]
    total_row_upd = len(modifiedexceptnew)
    text_upd = f'total row update except new : {total_row_upd}'
    print(text_upd)
    print(modifiedexceptnew)
    print('\n')

    targetnew = target[target['StatusCode'] == 'new']
    modifiednew = changes[changes[['FormNumber','ObjectID','OrderNo']].apply(tuple,1).isin(targetnew[['FormNumber','ObjectID','OrderNo']].apply(tuple,1))]
    total_row_upd = len(modifiednew)
    text_upd = f'total row update new : {total_row_upd}'
    print(text_upd)
    print(modifiednew)
    print('\n')

    # ambil data yang new dari changes
    inserted = changes[~changes[['FormNumber','ObjectID','OrderNo']].apply(tuple,1).isin(target[['FormNumber','ObjectID','OrderNo']].apply(tuple,1))]
    total_row_ins = len(inserted)
    text_ins = f'total row inserted : {total_row_ins}'
    print(text_ins)
    print(inserted)
    print('\n')

    if modifiedexceptnew.empty & modifiednew.empty & inserted.empty:
        print('tidak ada data yang bisa diproses karena tidak ada data baru atau update')

    elif modifiedexceptnew.empty & modifiednew.empty:
        # bikin tanggal sekarang buat kolom InsertDateStaging
        today = dt.datetime.now()
        today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
        inserted['InsertDateStaging'] = today_convert
        inserted.to_sql('TransFormulirLab', schema='staging_rscm', con = conn_staging_sqlserver, if_exists = 'append', index=False)
        print('success insert all data without update')
    
    else:
        #buat fungsi untuk update data ke tabel target
        def updated_to_except_new(df, table_name, key_1,key_2,key_3,key_4,key_5,key_6):
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
                if col == pk_1 or col == pk_2 or col == pk_3 or col == pk_4 or col == pk_5 or col == pk_6:
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

        def updated_to_new(df, table_name, key_1,key_2,key_3,key_4,key_5):
            list_col = []
            table=table_name
            pk_1 = key_1
            pk_2 = key_2
            pk_3 = key_3
            pk_4 = key_4
            pk_5 = key_5
            temp_table = f'{table}_temporary_table'
            for col in df.columns:
                if col == pk_1 or col == pk_2 or col == pk_3 or col == pk_4 or col == pk_5 :
                    continue
                list_col.append(f'r.{col} = t.{col}')
            df.to_sql(temp_table,schema = 'staging_rscm',con=conn_staging_sqlserver, if_exists='replace',index=False)
            update_stmt_1 = f'UPDATE r '
            update_stmt_2 = f'SET '
            update_stmt_3 = ", ".join(list_col)
            update_stmt_8 = f' , r.UpdateDateStaging = CONVERT(DATETIME2(0), GETDATE(),120)'
            update_stmt_4 = f' FROM staging_rscm.{table} r '
            update_stmt_5 = f'INNER JOIN (SELECT * FROM staging_rscm.{temp_table}) as t ON r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} AND r.{pk_3} = t.{pk_3} AND r.{pk_4} = t.{pk_4} AND r.{pk_5} = t.{pk_5} '
            update_stmt_6 = f'WHERE r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} AND r.{pk_3} = t.{pk_3} AND r.{pk_4} = t.{pk_4} AND r.{pk_5} = t.{pk_5} '
            update_stmt_7 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_8 + update_stmt_4 + update_stmt_5 + update_stmt_6 +";"
            delete_stmt_1 = f'DROP TABLE staging_rscm.{temp_table}'
            print(update_stmt_7)
            print(delete_stmt_1)
            conn_staging_sqlserver.execute(update_stmt_7)
            conn_staging_sqlserver.execute(delete_stmt_1)
            print('\n')

        try:
            # update data yang status codenya new
            updated_to_new(modifiednew, 'TransFormulirLab', 'PatientID','AdmissionID','FormNumber', 'ObjectID','OrderNo')
     
            # update data yang status codenya selain new
            updated_to_except_new(modifiedexceptnew, 'TransFormulirLab',  'PatientID','AdmissionID','FormNumber','ObjectID', 'OrderID','OrderNo')
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

db_connection.close_connection(conn_ehr)
db_connection.close_connection(conn_staging_sqlserver)
db_connection.close_connection(conn_his_live)