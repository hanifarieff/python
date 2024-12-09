import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pandas as pd
import pyodbc
import numpy as np
import datetime as dt
date = dt.datetime.today()
import time
import sys

sys.stdout = open("C:/TestPython/DWH_SQL_Server/Staging/logs/LogTransFormPathologyAnatomyPatmol.txt","w")

t0 = time.time()

conn_ehr = db_connection.create_connection(db_connection.replika_ehr)
conn_his = db_connection.create_connection(db_connection.replika_his)
conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)

source = pd.read_sql_query(""" 
                           SELECT  
                                lab.patient_id PatientID,
                                lab.admission_id AdmissionID,
                                lab.form_number FormNumber,
                                lab.no_pa_id RegistrationNo, -- No PA.,
                                lab.status_order as FormStatus,
                                'Patologi Molekuler' as Type,
                                CASE
                                    WHEN btr.trnsct_dttm IS NULL THEN objehr.obj_id_ehr
                                    ELSE pa.obj_id
                                END AS ObjID,
                                obj.obj_nm as ObjName, 
                                --  nama pasien nanti dari MPI,
                                lab.dr_employee_id DoctorID,
                                lab.dr_nm DoctorName,
                                fk.nama_faskes as Hospital,
                                lab.created_dttm as FormCreatedDate,
                                adm.org_id OrgID,-- Org dari admisi,
                                org.parent_id ParentOrgID, -- ParentID,
                                btr.billing_trnsct_id as BillingTransactID,
                                ord.billing_id as BillingID,
                                adm.admission_dttm AdmissionDate, -- tanggal admisi
                                btr.trnsct_dttm TransactionDate, -- ambil yang final
                                -- join ke FactPathologyAnatomy ambil kolom pemeriksaan  buat CheckType,
                                adm.payplan_id as PayplanID, -- buat join ke DimPayplan
                                -- Jenis Pasien nanti join ke Fact Patient Stay,
                                -- Join ke FactPathologyAnatomy ambil DPJP,
                                -- Join ke FactPathologyAnatomy ambil final date,
                                -- Join ke FactPathologyAnatomy ambil created date,
                                --  umur ambil ke MPI,
                                -- kelamin ambil ke MPI,
                                -- Join ke FactPathologyAnatomy ambil Topo,
                                --  Join ke FactPathologyAnatomy ambil Morfo,
                                -- Join ke FactPathologyAnatomy ambil Kesimpulan,
                                -- Tindakan sama kaya pemeriksaan,
                                -- MRN ambil ke MPI
                                NULL AS TissueSource, -- asal jaringan,
                                lab.diag_klin as DiagnosticHistory,
                                lab.diag_pa as PADiagnose -- DiagnosisPA hanya untuk imuno dan patmol
								
                                -- DPJP sama dengan SPV
                                -- Join ke FactPathologyAnatomy ambil Keterangan 
                            FROM xocp_his_pa_form_patologi_molekuler lab
                            INNER JOIN xocp_his_pa_order pa on pa.form_number = lab.form_number and lab.patient_id = pa.patient_id and pa.admission_id = lab.admission_id and pa.obj_id != '' and pa.status_cd='approved'
                            LEFT JOIN xocp_his_patient_admission adm on lab.patient_id = adm.patient_id AND lab.admission_id = adm.admission_id
                            LEFT JOIN xocp_orgs org on adm.org_id = org.org_id
                            LEFT JOIN xocp_his_patient_order ord on ord.patient_id = lab.patient_id and ord.admission_id = lab.admission_id
                            and ord.obj_id = pa.obj_id and ord.billing_paid = '1'
                            LEFT JOIN xocp_his_billing_trnsct btr on btr.billing_id = ord.billing_id
                            LEFT JOIN xocp_obj obj on obj.obj_id = pa.obj_id
                            LEFT JOIN xocp_obj_mapping_ehr objehr on objehr.obj_id_his =  pa.obj_id
                            LEFT JOIN xocp_his_bpjs_faskes fk on fk.kode_faskes = lab.id_rs_luar 
                            where 
                            lab.created_dttm >= '2024-10-16 00:00:00' and lab.created_dttm <= '2024-10-31 23:59:59'
 							-- and btr.trnsct_dttm IS NOT NULL
                            -- and fk.nama_faskes IS NOT NULL
                            -- lab.patient_id = 1054602 and lab.admission_id = 10
                            -- and adm.payplan_id NOT IN('8','71')
                            -- AND lab.no_pa_id IN ('M24-00034')
                            GROUP BY 
                                lab.patient_id,
                                lab.admission_id,
                                lab.form_number,
                                lab.no_pa_id,
                                lab.status_order,
                                pa.obj_id,
                                objehr.obj_id_ehr,
                                obj_nm,
                                lab.dr_employee_id,
                                lab.dr_nm,
                                lab.created_dttm,
                                adm.org_id,
                                org.parent_id,
                                btr.billing_trnsct_id,
                                ord.billing_id,
                                adm.admission_dttm,
                                btr.trnsct_dttm,
                                adm.payplan_id,
                                lab.diag_klin,
								lab.diag_pa
																
																    
                    """, conn_his)

print(source)
source['BillingTransactID'] = source['BillingTransactID'].replace(np.nan,0).astype('int64')
source['BillingID'] = source['BillingID'].replace(np.nan,0).astype('int64')
source['PayplanID'] = source['PayplanID'].astype('str')
# bikin function remove comma jika ada variable yang isinya cuma 1, variable ini akan di pakai IN CLAUSE di query target
def remove_comma(x):
    if len(x) == 1:
        return str(x).replace(',','')
    else:
        return x
    
# filter yang transaction date nya null karena itu billing dari EHR
source_ehr = source[source['TransactionDate'].isna()]

source_ehr['ObjID'].fillna('-',inplace=True)
if not source_ehr.empty:

    patient_id = tuple(source_ehr['PatientID'].unique())
    admission_id = tuple(source_ehr['AdmissionID'].unique())
    obj_id = tuple(source_ehr['ObjID'].unique())

    patient_id = remove_comma(patient_id)
    admission_id = remove_comma(admission_id)
    obj_id =remove_comma(obj_id)

    query_get_billing_ehr = f""" SELECT 
                                    patient_id PatientID,
                                    admission_id AdmissionID,
                                    obj_id ObjID,
                                    CASE
                                        WHEN bpjs.trnsct_id IS NULL THEN umum.trnsct_id
                                        ELSE bpjs.trnsct_id
                                    END AS BillingTransactIDNull,
                                    CASE
                                        WHEN ord.billing_id IS NULL or ord.billing_id = '' THEN '0'
				                        ELSE ord.billing_id
                                    END AS BillingIDNull,
                                    CASE
                                        WHEN bpjs.trnsct_dttm IS NULL THEN umum.trnsct_dttm
                                        ELSE bpjs.trnsct_dttm
                                    END AS TransactionDateNull
                                from xocp_ehr_patient_order ord
                                left join xocp_ehr_billing_trnsctpt bpjs on bpjs.billing_id = ord.billing_id
                                left join xocp_ehr_billing_trnsct umum on umum.billing_id = ord.billing_id
                                where patient_id IN {patient_id} and admission_id IN {admission_id} and obj_id IN {obj_id}
                                and ord.status_cd NOT IN ('nullified')"""
    billing_ehr = pd.read_sql_query(query_get_billing_ehr,conn_ehr)
    
    source = source.merge(billing_ehr, how='left',on=['PatientID','AdmissionID','ObjID'])
    source['TransactionDate'].fillna(source['TransactionDateNull'],inplace=True)
    source.loc[source['BillingTransactID'] == 0, 'BillingTransactID'] = source['BillingTransactIDNull']
    source.loc[source['BillingID'] == 0, 'BillingID'] = source['BillingIDNull']
    source= source.drop(columns=['BillingIDNull','BillingTransactIDNull','TransactionDateNull'])
    print(source.loc[:,['PatientID','AdmissionID','TransactionDate','BillingID','BillingTransactID']])

    # convert tipe data BillingID dan BillingTransactID ke string
    source['BillingID']=source['BillingID'].replace(np.nan,0).astype('str')
    source['BillingTransactID']=source['BillingTransactID'].replace(np.nan,0).astype('str')
    source['BillingID']=source['BillingID'].fillna('0').astype('str')
    source['BillingTransactID']=source['BillingTransactID'].fillna('0').astype('str')

    source.drop_duplicates(subset=['PatientID','AdmissionID','FormNumber','ObjID','BillingTransactID'],inplace=True)
    print('data source setelah join ke billing yang dari EHR')
else:
    pass

# convert tipe data BillingID dan BillingTransactID ke string
source['BillingID']=source['BillingID'].replace(np.nan,0).astype('str')
source['BillingTransactID']=source['BillingTransactID'].replace(np.nan,0).astype('str')
source['BillingID']=source['BillingID'].fillna('0').astype('str')
source['BillingTransactID']=source['BillingTransactID'].fillna('0').astype('str')

print('fix source data types')
print(source.dtypes)

source['ObjID'].fillna('-',inplace=True)
# source['ObsValue'] = source['ObsValue'].replace('\t', ' ').replace('\\n','\n')
# source['FinalUserID'] = source['FinalUserID'].replace(np.nan,0).astype('int64')
# source['ObjID'].replace({np.nan:0}, inplace=True)

# source=source.drop(columns='ObsValue')

if source.empty:
    print('tidak ada data dari source')
else:
    # ambil primary key dari source, pake unique biar tidak duplicate
    patient_id = tuple(source["PatientID"].unique())
    admission_id = tuple(source["AdmissionID"].unique())
    form_number = tuple(source["FormNumber"].unique())
    obj_id = tuple(source["ObjID"].unique())
    billing_transact_id = tuple(source["BillingTransactID"].unique())

    
    patient_id = remove_comma(patient_id)
    admission_id = remove_comma(admission_id)
    form_number = remove_comma(form_number)
    obj_id = remove_comma(obj_id)
    billing_transact_id = remove_comma(billing_transact_id)

    
    # query buat narik data dari target lalu filter berdasarkan primary key
    query = f"""SELECT 
                    PatientID,
                    AdmissionID,
                    FormNumber,
                    RegistrationNo,
                    FormStatus,
                    Type,
                    ObjID,
                    ObjName,
                    DoctorID,
                    DoctorName,
                    Hospital,
                    FormCreatedDate,
                    OrgID,
                    ParentOrgID,
                    BillingTransactID,
                    BillingID,
                    AdmissionDate,
                    TransactionDate,
                    PayplanID,
                    TissueSource,
                    DiagnosticHistory,
                    PADiagnose
                FROM staging_rscm.TransFormPathologyAnatomy WHERE PatientID IN {patient_id} AND 
                AdmissionID IN {admission_id} AND FormNumber IN {form_number} AND ObjID IN {obj_id} AND BillingTransactID IN {billing_transact_id}
            """
    target = pd.read_sql_query(query, conn_staging_sqlserver)

    print(target.dtypes)
    # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

    # ambil data yang update
    modified = changes[changes[['PatientID','AdmissionID','FormNumber','ObjID','BillingTransactID']].apply(tuple,1).isin(target[['PatientID','AdmissionID','FormNumber','ObjID','BillingTransactID']].apply(tuple,1))]
    total_row_upd = len(modified)
    text_upd = f'total row update : {total_row_upd}'
    print(text_upd)
    print(modified)

    #ambil data yang baru
    inserted = changes[~changes[['PatientID','AdmissionID','FormNumber','ObjID','BillingTransactID']].apply(tuple,1).isin(target[['PatientID','AdmissionID','FormNumber','ObjID','BillingTransactID']].apply(tuple,1))]
    total_row_ins = len(inserted)
    text_ins = f'total row inserted : {total_row_ins}'
    print(text_ins)
    print(inserted)

    if modified.empty:
        if inserted.empty :
            print('tidak ada data yang baru dan berubah')
        else:
            today = dt.datetime.now()
            today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
            inserted['InsertDateStaging'] = today_convert
            inserted.to_sql('TransFormPathologyAnatomy', schema='staging_rscm' ,con=conn_staging_sqlserver, if_exists = 'append', index=False)
            print('success insert without update')
    else:
        # buat fungsi update data
        def updated_data(df, table_name, key_1,key_2,key_3,key_4,key_5):
            a = []
            table = table_name
            pk_1 = key_1
            pk_2 = key_2
            pk_3 = key_3
            pk_4 = key_4
            pk_5 = key_5
            temp_table = f'{table}_temporary_table'
            for col in df.columns:
                if col == pk_1 or col == pk_2 or col == pk_3 or col == pk_4 :
                    continue
                a.append(f't.{col} = s.{col}')
            df.to_sql(temp_table, schema='staging_rscm', con=conn_staging_sqlserver, if_exists = 'replace', index = False)
            update_stmt_1 = f'UPDATE t '
            update_stmt_2 = f'SET '
            update_stmt_3 = ", ".join(a)
            update_stmt_8 = f' , t.UpdateDateStaging = CONVERT(DATETIME2(0), GETDATE(),120)'
            update_stmt_4 = f' FROM staging_rscm.{table} t '
            update_stmt_5 = f'INNER JOIN (SELECT * FROM staging_rscm.{temp_table}) AS s ON t.{pk_1} = s.{pk_1} AND t.{pk_2} = s.{pk_2} AND t.{pk_3} = s.{pk_3} AND t.{pk_4} = s.{pk_4} AND t.{pk_5} = s.{pk_5} '
            update_stmt_6 = f' WHERE t.{pk_1} = s.{pk_1}  AND t.{pk_2} = s.{pk_2} AND t.{pk_3} = s.{pk_3} AND t.{pk_4} = s.{pk_4} AND t.{pk_5} = s.{pk_5} '
            update_stmt_7 = update_stmt_1 +  update_stmt_2 + update_stmt_3 + update_stmt_8 + update_stmt_4 + update_stmt_5 + update_stmt_6 +";"
            delete_stmt_1 = f'DROP TABLE staging_rscm.{temp_table} '
            print(update_stmt_7)
            conn_staging_sqlserver.execute(update_stmt_7)
            conn_staging_sqlserver.execute(delete_stmt_1)

        try:
            #update data
            updated_data(modified, 'TransFormPathologyAnatomy', 'PatientID','AdmissionID','FormNumber','BillingTransactID')

            #insert data
            today = dt.datetime.now()
            today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
            inserted['InsertDateStaging'] = today_convert
            inserted.to_sql('TransFormPathologyAnatomy', schema='staging_rscm', con=conn_staging_sqlserver, if_exists = 'append', index=False)

            print('all success updated and inserted')
        except Exception as e:
            print(e)

#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)
print('\n')
text=f'scheduler tanggal : {date}'
print(text)

db_connection.close_connection(conn_his)
db_connection.close_connection(conn_staging_sqlserver)
sys.stdout.close()


