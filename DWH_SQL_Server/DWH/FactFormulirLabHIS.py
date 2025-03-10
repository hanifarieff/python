import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pyodbc
import pandas as pd
import time
import datetime as dt
date = dt.datetime.today()

# bikin log ke file
sys.stdout = open("C:/TestPython/DWH_SQL_Server/DWH/logs/LogFactFormulirLabHIS.txt","w")
t0 = time.time()

conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)
conn_dwh_sqlserver = db_connection.create_connection(db_connection.dwh_sqlserver)

source = pd.read_sql_query("""
                          SELECT 
                                a.PatientID,
                                a.AdmissionID,
                                a.FormNumber,
                                a.OrgID,
                                c.OrganizationSurrogateKey as OrgIDSurrogateKeyID,
                                TRIM(a.ObjectID) as ObjectID,
                                a.ObjectName as ObjectName,
                                a.AdmissionDate as AdmissionDate,
                                a.CreatedDate as CreatedDate,
                                a.CreatedBy as CreatedBy,
                                a.StatusCode as StatusCode,
                                TRIM(a.OrderLab) as OrderLab,
                                TRIM(a.OrderID) as OrderID,
                                a.PriorityPatient as PriorityPatient,
                                a.DoctorEmployeeID as DoctorEmployeeID,
                                a.VerifiedBy as VerifiedBy,
                                a.VerifiedDate as VerifiedDate,
                                a.RejectedBy as RejectedBy,
                                a.RejectedDate as RejectedDate,
                                a.ApprovedBy as ApprovedBy,
                                a.ApprovedDate as ApprovedDate,
                                a.CancelledBy as CancelledBy,
                                a.CancelledDate as CancelledDate,
                                a.DstOrg as DstOrg,
                                d.OrganizationSurrogateKey as DstOrgSurrogateKeyID,
                                a.ApprovalOrg as ApprovalOrg,
                                e.OrganizationSurrogateKey as ApprovalOrgSurrogateKeyID,
                                TRIM(a.Diagnose) as Diagnose,
                                TRIM(a.AddedNotes) as AddedNotes,
                                TRIM(a.Modality) as Modality,
                                a.PatientStatus as PatientStatus,
                                a.FastingStatus as FastingStatus,
                                a.UrineStatus as UrineStatus,
                                a.ThalasemiaStatus as ThalasemiaStatus,
                                a.LiquidSamplingStatus as LiquidSamplingStatus,
                                a.OperationPreparationStatus as OperationPreparationStatus,
                                TRIM(a.OrderNo) as OrderNo,
                                a.OrderIDRME as OrderIDRME,
                                a.ScheduleID as ScheduleID,
                                a.Flag as Flag,
                                a.FormLabDetailID
                            FROM staging_rscm.TransFormulirLab a
                            LEFT JOIN staging_rscm.DimensionPatientMPI b on a.PatientID = b.PatientID and b.ScdActive = 1
                            LEFT JOIN staging_rscm.DimensionOrganization c on a.OrgID = c.ChildOrganizationID and c.SCDActive = 1
                            LEFT JOIN staging_rscm.DimensionOrganization d on a.DstOrg = d.ChildOrganizationID and d.SCDActive = 1
                            LEFT JOIN staging_rscm.DimensionOrganization e on a.ApprovalOrg = e.ChildOrganizationID and e.SCDActive = 1
                            WHERE 
                            b.MedicalNo NOT IN (SELECT MedicalNo FROM StagingRSCM.staging_rscm.DimensionDummyPatient)
                            AND a.Flag = 2 
							AND
                            -- CAST(a.CreatedDate as date) = '2023-12-03'
                            (CAST(a.InsertDateStaging as date) >= CAST(DATEADD(DAY, -1, GETDATE()) as date) AND CAST(a.InsertDateStaging as date) <= CAST(GETDATE() as date) 
                            OR CAST(a.UpdateDateStaging as date) >= CAST(DATEADD(DAY, -2, GETDATE()) as date) AND CAST(a.UpdateDateStaging as date) <= CAST(GETDATE() as date)) 
                            -- AND a.FormNumber IN (15473,15476) 
                            
                         """,con=conn_staging_sqlserver)
source.replace({pd.NaT: None},inplace=True)
source['Flag'] = 2
source['ApprovalOrgSurrogateKeyID']=source['ApprovalOrgSurrogateKeyID'].fillna(0).astype('int64')
source['OrderIDRME']=source['OrderIDRME'].fillna(0).astype('int64')
source['ScheduleID']=source['ScheduleID'].fillna(0).astype('int64')
print(source)

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
        query = f"SELECT PatientID,AdmissionID,FormNumber,OrgID,OrgIDSurrogateKeyID, TRIM(ObjectID) as ObjectID,ObjectName, AdmissionDate, CreatedDate,CreatedBy,StatusCode,TRIM(OrderLab) as OrderLab,TRIM(OrderID) AS OrderID,PriorityPatient,DoctorEmployeeID, VerifiedBy,VerifiedDate,RejectedBy,RejectedDate,ApprovedBy,ApprovedDate,CancelledBy,CancelledDate,DstOrg,DstOrgSurrogateKeyID,ApprovalOrg,ApprovalOrgSurrogateKeyID,TRIM(Diagnose) as Diagnose,TRIM(AddedNotes) as AddedNotes,TRIM(Modality) as Modality,PatientStatus,FastingStatus,UrineStatus,ThalasemiaStatus,LiquidSamplingStatus,OperationPreparationStatus,TRIM(OrderNo) as OrderNo,OrderIDRME,ScheduleID,Flag,FormLabDetailID from dwhrscm_taled.FactFormulirLab where FormNumber IN ({formnumber}) AND ObjectID IN({objid}) AND OrderID IN ({orderid}) AND OrderNo IN ({orderno}) AND Flag = 2 order by PatientID,AdmissionID"
        target = pd.read_sql_query(query, conn_dwh_sqlserver)
        
    else :
         # ambil primary key dari source, pake unique biar tidak duplicate
        formnumber = tuple(source["FormNumber"])
        objid = tuple(source["ObjectID"].unique())
        orderid = tuple(source["OrderID"].unique())
        orderid = orderid + (' ',)
        orderno = tuple(source["OrderNo"])
        formlabdetailid = tuple(source["FormLabDetailID"].unique())

         # query buat narik data dari target lalu filter berdasarkan primary key
        query = f"SELECT PatientID,AdmissionID,FormNumber,OrgID,OrgIDSurrogateKeyID, TRIM(ObjectID) as ObjectID,ObjectName, AdmissionDate, CreatedDate,CreatedBy,StatusCode,TRIM(OrderLab) as OrderLab,TRIM(OrderID) AS OrderID,PriorityPatient,DoctorEmployeeID, VerifiedBy,VerifiedDate,RejectedBy,RejectedDate,ApprovedBy,ApprovedDate,CancelledBy,CancelledDate,DstOrg,DstOrgSurrogateKeyID,ApprovalOrg,ApprovalOrgSurrogateKeyID,TRIM(Diagnose) as Diagnose,TRIM(AddedNotes) as AddedNotes,TRIM(Modality) as Modality,PatientStatus,FastingStatus,UrineStatus,ThalasemiaStatus,LiquidSamplingStatus,OperationPreparationStatus,TRIM(OrderNo) as OrderNo,OrderIDRME,ScheduleID,Flag,FormLabDetailID from dwhrscm_talend.FactFormulirlab where FormNumber IN {formnumber} AND ObjectID IN {objid} AND OrderID IN {orderid} AND OrderNo IN {orderno} AND Flag = 2 order by PatientID,AdmissionID"
        target = pd.read_sql_query(query, conn_dwh_sqlserver)

        target['ApprovalOrgSurrogateKeyID']=target['ApprovalOrgSurrogateKeyID'].fillna(0).astype('int64')
        target['OrderIDRME']=target['OrderIDRME'].fillna(0).astype('int64')
        target['ScheduleID']=target['ScheduleID'].fillna(0).astype('int64')
        # karena ada beberapa tanggal yang kosong, ubah format tanggal NaT menjadi None
        target.replace({pd.NaT: None},inplace=True)
    print(source.dtypes)
    print(target.dtypes)
    # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]
   
    print('ini changes')
    print(changes)
    print('\n')

    # ambil data yang update dari changes
    targetexceptnew = target[target['StatusCode'] != 'new']
    modifiedexceptnew = changes[changes[['PatientID', 'AdmissionID','FormNumber','ObjectID','OrderID','OrderNo']].apply(tuple,1).isin(targetexceptnew[['PatientID','AdmissionID','FormNumber','ObjectID','OrderID','OrderNo']].apply(tuple,1))]
    total_row_upd = len(modifiedexceptnew)
    text_upd = f'total row update except new : {total_row_upd}'
    print(text_upd)
    print(modifiedexceptnew)
    print('\n')

    targetnew = target[target['StatusCode'] == 'new']
    modifiednew = changes[changes[['PatientID','AdmissionID','FormNumber','ObjectID','OrderNo']].apply(tuple,1).isin(targetnew[['PatientID','AdmissionID','FormNumber','ObjectID','OrderNo']].apply(tuple,1))]
    total_row_upd = len(modifiednew)
    text_upd = f'total row update new : {total_row_upd}'
    print(text_upd)
    print(modifiednew)
    print('\n')

    # ambil data yang new dari changes
    inserted = changes[~changes[['PatientID','AdmissionID','FormNumber','ObjectID','OrderNo']].apply(tuple,1).isin(target[['PatientID','AdmissionID','FormNumber','ObjectID','OrderNo']].apply(tuple,1))]
    total_row_ins = len(inserted)
    text_ins = f'total row inserted : {total_row_ins}'
    print(text_ins)
    print(inserted)
    print('\n')

    if modifiedexceptnew.empty & modifiednew.empty & inserted.empty:
        print('tidak ada data yang bisa diproses karena data yang berubah dan baru kosong')

    elif modifiedexceptnew.empty & modifiednew.empty:
        # bikin tanggal sekarang buat kolom InsertDateStaging
        today = dt.datetime.now()
        today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
        inserted['InsertedDateDWH'] = today_convert
        inserted.to_sql('FactFormulirLab', schema='dwhrscm_talend', con = conn_dwh_sqlserver, if_exists = 'append', index=False)
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
                if col == pk_1 or col == pk_2 or col == pk_3 or col == pk_4 or col == pk_5 or col ==pk_6:
                    continue
                list_col.append(f'r.{col} = t.{col}')
            df.to_sql(temp_table,schema = 'dwhrscm_talend',con=conn_dwh_sqlserver, if_exists='replace',index=False)
            update_stmt_1 = f'UPDATE r '
            update_stmt_2 = f'SET '
            update_stmt_3 = ", ".join(list_col)
            update_stmt_8 = f' , r.UpdateDateDWH = CONVERT(DATETIME2(0), GETDATE(),120)'
            update_stmt_4 = f' FROM dwhrscm_talend.{table} r '
            update_stmt_5 = f'INNER JOIN (SELECT * FROM dwhrscm_talend.{temp_table}) as t ON r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} AND r.{pk_3} = t.{pk_3} AND r.{pk_4} = t.{pk_4} AND r.{pk_5} = t.{pk_5} AND r.{pk_6} = t.{pk_6} '
            update_stmt_6 = f'WHERE r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} AND r.{pk_3} = t.{pk_3} AND r.{pk_4} = t.{pk_4} AND r.{pk_5} = t.{pk_5} AND r.{pk_6} = t.{pk_6} '
            update_stmt_7 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_8 + update_stmt_4 + update_stmt_5 + update_stmt_6 +";"
            delete_stmt_1 = f'DROP TABLE dwhrscm_talend.{temp_table}'
            print(update_stmt_7)
            print(delete_stmt_1)
            conn_dwh_sqlserver.execute(update_stmt_7)
            conn_dwh_sqlserver.execute(delete_stmt_1)

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
            df.to_sql(temp_table,schema = 'dwhrscm_talend',con=conn_dwh_sqlserver, if_exists='replace',index=False)
            update_stmt_1 = f'UPDATE r '
            update_stmt_2 = f'SET '
            update_stmt_3 = ", ".join(list_col)
            update_stmt_8 = f' , r.UpdateDateDWH = CONVERT(DATETIME2(0), GETDATE(),120)'
            update_stmt_4 = f' FROM dwhrscm_talend.{table} r '
            update_stmt_5 = f'INNER JOIN (SELECT * FROM dwhrscm_talend.{temp_table}) as t ON r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} AND r.{pk_3} = t.{pk_3} AND r.{pk_4} = t.{pk_4} AND r.{pk_5} = t.{pk_5}  '
            update_stmt_6 = f'WHERE r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} AND r.{pk_3} = t.{pk_3} AND r.{pk_4} = t.{pk_4} AND r.{pk_5} = t.{pk_5}  '
            update_stmt_7 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_8 + update_stmt_4 + update_stmt_5 + update_stmt_6 +";"
            delete_stmt_1 = f'DROP TABLE dwhrscm_talend.{temp_table}'
            print(update_stmt_7)
            print(delete_stmt_1)
            conn_dwh_sqlserver.execute(update_stmt_7)
            conn_dwh_sqlserver.execute(delete_stmt_1)
            print('\n')

        try:
            # update data yang status codenya new
            updated_to_new(modifiednew, 'FactFormulirLab', 'PatientID','AdmissionID','FormNumber', 'ObjectID','OrderNo')
     
            # update data yang status codenya selain new
            updated_to_except_new(modifiedexceptnew, 'FactFormulirLab', 'PatientID','AdmissionID','FormNumber','ObjectID', 'OrderID','OrderNo')
            # insert data baru
            today = dt.datetime.now()
            today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
            inserted['InsertedDateDWH'] = today_convert
            inserted.to_sql('FactFormulirLab', schema='dwhrscm_talend', con=conn_dwh_sqlserver, if_exists ='append',index=False)
            print('success update and insert all data')
        except Exception as e:
            print(e)

#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)

text=f'scheduler tanggal : {date}'
print(text)

db_connection.close_connection(conn_staging_sqlserver)
db_connection.close_connection(conn_dwh_sqlserver)