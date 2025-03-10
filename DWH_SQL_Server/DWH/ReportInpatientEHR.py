import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import sys
import time

sys.stdout = open("C:/TestPython/DWH_SQL_Server/DWH/logs/LogReportInpatient.txt","w")
t0 = time.time()

conn_ehr = db_connection.create_connection(db_connection.replika_ehr)
conn_dwh_sqlserver=db_connection.create_connection(db_connection.dwh_sqlserver)

# define tanggal penarikan, biasanya tiap akhir bulan sesuai request mas hariri
date = f"2024-09-30"

query_source = f""" select 
                            a.OrderID, 
                            a.PatientID, 
                            a.AdmissionID, 
                            c.PatientName,
                            c.MedicalNo,
                            a.AdmissionDate, 
                            a.ProcName,
                            a.LocationNow as Ruangan,
                            '{date}' as TanggalReport,
                            b.PayplanNo as SEP
                        from dwhrscm_talend.[127DMInpatientMutation] a
                        left join dwhrscm_talend.FactPatientAdmission b on a.PatientID = b.PatientID and a.AdmissionID = b.AdmissionID
                        left join dwhrscm_talend.DimPatientMPI c on a.PatientID = c.PatientID and c.ScdActive = '1'
                        where ((a.AdmissionDate <= '{date} 23:59:59' AND (a.DischargeDate > '{date} 23:59:59' OR a.DischargeDate IS NULL))
                        OR
                        a.AdmissionDate >= '{date} 00:00:00' AND a.DischargeDate <= '{date} 23:59:59' )
                        and b.PayPlanID != 71 and a.OrderID LIKE '00%'
            """
source = pd.read_sql_query(query_source, conn_dwh_sqlserver)

print(source)
patientid = tuple(source['PatientID'])
admissionid = tuple(source['AdmissionID'])

# bikin function remove comma jika ada variable yang isinya cuma 1, variable ini akan di pakai IN CLAUSE di query target
def remove_comma(x):
    if len(x) == 1:
        return str(x).replace(',','')
    else:
        return x

patientid = remove_comma(patientid)
admissionid = remove_comma(admissionid)

query_order = f"""select 
                    a.patient_id as PatientID,
                    a.admission_id as AdmissionID,
                    CAST(a.tariff as decimal(15,2)) as Tarif,
                    CAST(d.ttl_amount as decimal(30,2)) as TarifTotal,
                    CASE
                            WHEN e.obj_id LIKE 'MSRV%%' THEN 'akomodasi'
                            WHEN c.eklaim_group_nm IS NULL THEN 'prosedur_non_bedah'
                            ELSE c.eklaim_group_nm 
                    END AS EklaimGroup,
                    CASE 
                        WHEN a.payplan_id = 8 THEN 'pribadi'
                        ELSE 'perusahaan'
                    END AS Jaminan
                from xocp_ehr_patient_order a
                left join xocp_ehr_billing d on a.billing_id = d.billing_id
                left join xocp_ehr_order_group b on a.obj_id = b.obj_id
                left join xocp_eklaim_group c on b.eklaim_group_id = c.eklaim_group_id
                left join xocp_ehr_obj e on a.obj_id = e.obj_id
                where a.patient_id IN {patientid} and a.admission_id IN {admissionid}
                and a.status_cd NOT IN ('nullified','cancelled') """

order = pd.read_sql_query(query_order,conn_ehr)

query_obat = f""" SELECT 
                patient_id as PatientID,
                admission_id as AdmissionID,
                CASE
                    WHEN payor_pay = 0 then 
                        CASE
                            WHEN patient_pay = '' THEN 0
                            ELSE patient_pay
                        END
                    ELSE 
                        CASE
                            WHEN payor_pay = '' THEN 0
                            ELSE payor_pay
                        END
                END AS Tarif,
                NULL as TarifTotal,
                'obat' as EklaimGroup
                FROM xocp_ehr_ff_order
                WHERE patient_id in {patientid} and admission_id in {admissionid}"""
obat = pd.read_sql_query(query_obat,conn_ehr)

order_obat = pd.concat([order,obat], ignore_index=True)

print(order_obat)

source = source.merge(order_obat,how='left',on=['PatientID','AdmissionID'])
mapping_dict = source.groupby(['PatientID']).first().to_dict()
source['TarifTotal'] = source['TarifTotal'].fillna(source['PatientID'].map(mapping_dict['TarifTotal']))
source['Jaminan'] = source['Jaminan'].fillna(source['PatientID'].map(mapping_dict['Jaminan']))

source['Tarif'] = source['Tarif'].replace('',0).astype('float64')
print(source)
print(source.isna().any())
print(source.dtypes)

try:
    with conn_dwh_sqlserver.begin() as transaction:
        source.to_sql('ReportInpatient', schema='dwhrscm_talend',con=conn_dwh_sqlserver, if_exists ='append',index=False)
        print('insert all data')
except Exception as e:
	print(e)

#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)
# ambil primary key dari source
# period = tuple(source["Period"])
# unit = tuple(source["UnitID"])
# # print(period)
# # query buat narik data dari target lalu filter berdasarkan primary key
# query = f'SELECT * FROM FactRevenuePerUnit where Period IN {period} AND UnitID IN {unit} order by Period asc,UnitID asc'
# target = pd.read_sql_query (query, conn_dwh)
# target['Period'] = pd.to_datetime(target['Period'])
# # target['Revenue'] = target['Revenue'].astype(str) 
# print(target.dtypes)

# # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
# changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

# # data yang terupdate
# modified = changes[changes[['Period','UnitID']].apply(tuple,1).isin(target[['Period','UnitID']].apply(tuple,1))]
# print('ini update')
# print(modified)
# # data baru
# inserted =  changes[~changes[['Period','UnitID']].apply(tuple,1).isin(target[['Period','UnitID']].apply(tuple,1))]
# print('ini data baru')
# print(inserted)

# # buat function untuk update data
# def updated_to_sql(df, table_name, key_1, key_2):
#     a = []
#     table = table_name
#     pk_1 = key_1
#     pk_2 = key_2
#     temp_table = f'{table}_temporary_table'
#     for col in df.columns:
#         if col == pk_1 or col == pk_2:
#             continue
#         a.append(f't.{col} = s.{col}')
#     df.to_sql(temp_table, con=conn_dwh, if_exists = 'replace', index=False)
#     update_stmt_1 = f'UPDATE {table} t '
#     update_stmt_2 = f' INNER JOIN (SELECT * FROM {temp_table}) AS s ON t.{pk_1} = s.{pk_1} AND t.{pk_2} = s.{pk_2} '
#     update_stmt_3 = f'SET '
#     update_stmt_4 = ", ".join(a)
#     update_stmt_5 = f' WHERE t.{pk_1} = s.{pk_1} AND t.{pk_2} = s.{pk_2}'
#     update_stmt_7 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_4 + update_stmt_5 + ";"
#     delete_stmt_1 = f'DROP TABLE {temp_table} '
#     print(update_stmt_7)
#     print('\n')
#     conn_dwh.execute(update_stmt_7)
#     conn_dwh.execute(delete_stmt_1)
# try: 
    
#     # panggil function update untuk proses update data ke target
#     updated_to_sql(modified, 'FactRevenuePerUnit', 'Period','UnitID')

#     # insert data baru ke target
#     inserted.to_sql('FactRevenuePerUnit', con = conn_dwh, if_exists='append', index=False)
#     print('successfully update and insert')
# except Exception as e:
#     print(e)


sys.stdout.close()