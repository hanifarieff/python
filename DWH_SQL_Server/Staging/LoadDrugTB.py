import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pandas as pd
import pyodbc
import datetime as dt
date = dt.datetime.today()
import time



# bikin koneksi ke db
# conn_his_live = db_connection.create_connection(db_connection.his_live)
# 
conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)


# tarik query, masuk ke variabel source
source = pd.read_excel(r'C:\TestPython\LIST OBAT TUBERKULOSIS.xlsx',sheet_name='Sheet1')

print('ini source')
print(source)

# source['TanggalPemeriksaan'] = pd.to_datetime(source.TanggalPemeriksaan, format='%Y-%m-%d')
# source['TanggalPemeriksaan'] = source['TanggalPemeriksaan'].dt.strftime('%Y-%m-%d')

# source_new = pd.DataFrame(columns=['PatientID','AdmissionID','MedicalNo','AdmissionDate'])

# query = f"""
#             SELECT
#                 a.PatientID,
#                 a.AdmissionID,
#                 a.MedicalNo,
#                 a.AdmissionDate
#             FROM dwhrscm_talend.FactPatientAdmission a
#             WHERE a.MedicalNo = ? and a.AdmissionDate = ?
#          """
# # looping untuk setiap rows di source
# for index, row in source.iterrows():
#     pk_values = (row['MRN'],row['TanggalPemeriksaan']) # ambil patient id dan admission id nya
#     # Ensure pk_values is a tuple with the correct data types
#     if not isinstance(pk_values, tuple):
#         pk_values = tuple(pk_values)
    
#     # jalankan query cek berdasarkan patientid dan admissionid dari stayind_mismatch
#     results = conn_dwh_sqlserver.execute(query,pk_values).fetchall()

#     # jika results ada isinya, maka akan masuk ke dataframe source_changes
#     if results:
#         result_df = pd.DataFrame.from_records(results, columns=source_new.columns)
#         source_new = pd.concat([source_new, result_df], ignore_index=True)
#     else:
#         print('the results is empty')

# print(source_new)

try:
    source.to_sql('ListDrugTB',schema='staging_rscm',con=conn_staging_sqlserver, if_exists='replace',index=False)
    print('ok')
except Exception as e:
    print(e)
# source.drop('requested_amount',axis=1,inplace=True)

# source['AdmissionDate'] = source['AdmissionDate'].dt.strftime('%Y-%m-%d')
# source['verified_dttm'] = pd.to_datetime(source['verified_dttm'] + ' 00:00:00', format="%Y-%m-%d %H:%M:%S")
# source['verified_dttm'] = source['verified_dttm'] + ' 00:00:00'
# source['verified_dttm'] = pd.to_datetime(source['verified_dttm'] )
# source=source[source['sep_no']=='0901R0010224V000019']
# source['AdmissionDate'] = source['AdmissionDate'].fillna(' ')


print(source.dtypes)
# try:
#     source.to_sql('ReferenceDiagnoseUronefro',schema='dwhrscm_talend',con=conn_dwh_sqlserver, if_exists='replace',index=False)
#     print('ok')
# except Exception as e:
#     print(e)

# query_cek = f""" select 
#     a.admission_dttm as AdmissionDate,
# 	b.patient_mrn_txt as MedicalNo,
# 	a.payplan_id PayplanID, 
# 	CASE
# 		WHEN a.admission_type = 'outpatient' then 'n'
# 		WHEN a.admission_type = 'inpatient' then 'y'
# 	END AS StayInd, 
#   c.location_id as LocationID
# from xocp_his_patient_admission a
# left join xocp_his_patient b on a.patient_id = b.patient_id
# left join xocp_his_patient_location c on a.patient_id = c.patient_id and a.admission_id = c.admission_id
# where b.patient_mrn_txt = %s
# and admission_dttm >= '2024-02-01 00:00:00' and admission_dttm <= '2024-02-28 23:59:59'
# """

# source_changes = pd.DataFrame(columns=['AdmissionDate','MedicalNo','StayInd','PayplanID','LocationID'])

# # looping untuk setiap rows di dataframe stayind_mismatch  (data2 yang berisi stayind yang tadinya y tapi berubah jadi n)
# for index, row in source.iterrows():
#     pk_values = row['MedicalNo'] # ambil patient id dan admission id nya
#     tuple(pk_values)
#     # # Ensure pk_values is a tuple with the correct data types
#     # if not isinstance(pk_values, tuple):
#     #     pk_values = tuple(pk_values)
    
#     # jalankan query cek berdasarkan patientid dan admissionid dari stayind_mismatch
#     results = conn_his_live.execute(query_cek,pk_values).fetchall()
#     print(results)

#     # jika results ada isinya, maka akan masuk ke dataframe source_changes
#     if results:
#         result_df = pd.DataFrame.from_records(results, columns=source_changes.columns)
#         source_changes = pd.concat([source_changes, result_df], ignore_index=True)
#     else:
#         print('the results is empty')

# source_changes.to_excel('hasil cek location id stay ind his-2.xlsx')
# print('ok')

# #     print(target)
# # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
# changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

# # ambil data yang update
# modified = changes[changes[['sep_no']].apply(tuple,1).isin(target[['sep_no']].apply(tuple,1))]
# total_row_upd = len(modified)
# text_upd = f'total row update : {total_row_upd}'
# print(text_upd)
# print(modified)

# #ambil data yang baru
# inserted = changes[~changes[['sep_no']].apply(tuple,1).isin(target[['sep_no']].apply(tuple,1))]
# total_row_ins = len(inserted)
# text_ins = f'total row inserted : {total_row_ins}'
# print(text_ins)
# print(inserted)

# if modified.empty:
#    pass
# else:
#     # buat fungsi untuk update
#     def updated_to_sql(df, table_name, key_1):
#         list_col = []
#         table = table_name
#         pk_1 = key_1
#         temp_table = f'{table}_temporary_table'
#         for col in df.columns:
#             if col == pk_1 :
#                 continue
#             list_col.append(f't.{col} = s.{col}')
#         df.to_sql(temp_table, con = conn_ehr_live, if_exists = 'append',index = False)
#         update_stmt_1 = f'UPDATE {table} t '
#         update_stmt_2 = f'INNER JOIN (SELECT * from {temp_table}) AS s ON t.{pk_1} = s.{pk_1}  '
#         update_stmt_3 = f'SET '
#         update_stmt_4 = ", ".join(list_col)
#         update_stmt_5 = f' WHERE t.{pk_1} = s.{pk_1} '
#         update_stmt_6 = update_stmt_1 + update_stmt_2 + update_stmt_3  + update_stmt_4 + update_stmt_5 +";"
#         # delete_stmt_1 = f'DROP TABLE {temp_table}'
#         print(update_stmt_6)
#         print('\n')
#         conn_ehr_live.execute(update_stmt_6)
#         # conn_ehr_live.execute(delete_stmt_1)


#     try:
#         # call fungsi update
#         updated_to_sql(modified, 'xocp_ehr_claim_jkn_item', 'sep_no')
#         # masukkan data yang baru ke table target
#         # inserted.to_sql('xocp_ehr_claim_jkn_item', con = conn_ehr_live, if_exists='append',index=False)
#         print('success update dan insert')
#     except Exception as e:


db_connection.close_connection(conn_staging_sqlserver)


sys.stdout.close()
