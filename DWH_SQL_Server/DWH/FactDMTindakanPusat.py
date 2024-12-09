from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine as ce
import pyodbc
import pandas as pd
import numpy as np
import time
import datetime as dt
date = dt.datetime.today()
import sys

sys.stdout = open("C:/TestPython/DWH_SQL_Server/DWH/logs/LogFactDMTindakanPusat.txt","w")
t0 = time.time()

ehr = ce("mysql://hanif-ppi:hanif2022@172.16.19.11/ehr")
staging_sqlserver = ce('mssql+pyodbc://dev-ppi:D3vpp122!@172.16.19.36/StagingRSCM?driver=SQL+Server+Native+Client+11.0', fast_executemany=True)
dwh_sqlserver = ce('mssql+pyodbc://dev-ppi:D3vpp122!@172.16.19.36/DWH_RSCM?driver=SQL+Server+Native+Client+11.0')

try:
    conn_ehr = ehr.connect()
    conn_staging_sqlserver = staging_sqlserver.connect()
    conn_dwh_sqlserver = dwh_sqlserver.connect()
    print('successfully connected')
except SQLAlchemyError as e:
    error = str(e.__dict__['orig'])
    print(error)


source = pd.read_sql_query(""" SELECT DISTINCT
                                        a.order_id as OrderID,
                                        a.patient_nm as PatientName,
                                        a.patient_ext_id as MedicalNo,
                                        a.admission_dttm as AdmissionDate,
                                        a.ordered_dttm as OrderDate,
                                        a.payplan_nm as PayplanName,
                                        a.employee_id as EmployeeID,
                                        a.employee_ext_id as EmployeeNo,
                                        e.id_card_num as IDCardNumber,
                                        a.employee_nm as EmployeeName,
                                        a.jasa_medis as JasaMedis,
                                        c.tariff as Tarif,
                                        a.hcp_nm as HCPName,
                                        f.org_nm as OrgName,
                                        a.bulan as Bulan,
                                        a.tahun as Tahun,
                                        a.proc_id as ProcID,
                                        a.proc_nm as ProcName,
                                        t.category_nm as CategoryName,
                                        b.payplan_id as PayplanID,
                                        'P' AS DMType,
                                        a.verified_dttm as VerifiedDate,
                                        CASE
                                            WHEN b.payplan_id = 71 THEN 'BPJS'
                                            ELSE 'Non BPJS'
                                        END AS PayplanKemkes
                                        
                                FROM
                                xocp_dm_tindakan AS a
                                -- LEFT JOIN xocp_ehr_patient_admission g on a.patient_id = g.patient_id and a.admission_id = g.admission_id
                                LEFT JOIN xocp_ehr_patient_order AS b ON a.order_id = b.order_id
                                LEFT JOIN xocp_ehr_payplan_obj AS c ON b.obj_id = c.obj_id AND b.payplan_id = c.payplan_id
                                LEFT JOIN xocp_hrm_employee AS d ON a.employee_id = d.employee_id AND a.employee_ext_id = d.employee_ext_id
                                LEFT JOIN (SELECT employee_id, id_card_num FROM xocp_hrm_emp_idcard WHERE idcard_type = '1' GROUP BY employee_id) e on d.employee_id = e.employee_id
                                LEFT JOIN xocp_orgs AS f ON d.employee_org_id = f.org_id
                                INNER JOIN xocp_ehr_obj_kemenkes AS t ON a.proc_id = t.obj_id
                                WHERE
                                a.verified_dttm >= '2023-09-01 00:00:00' AND
                                a.verified_dttm <= '2023-09-02 23:59:59'
                                """, conn_ehr)
source['DMType']='P'
print(source)
# source['JasaMedis']=source['JasaMedis'].astype(str)
source.replace({np.nan: None},inplace=True)
print(source.dtypes)
print(source.isna().any())
source.to_sql('FactDMTindakan', schema='dwhrscm_talend', con=conn_dwh_sqlserver, if_exists ='append',index=False)
print('success inserted')
# try:  
#     source.to_sql('FactDMTindakan', schema='dwhrscm_talend', con=conn_dwh_sqlserver, if_exists ='append',index=False)
#     print('success inserted')
# except Exception as e:
#     print(e)

#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)

text=f'scheduler tanggal : {date}'
print(text)

conn_ehr.close()
conn_dwh_sqlserver.close()
conn_staging_sqlserver.close()