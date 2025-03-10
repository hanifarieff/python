import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import sys
import time

sys.stdout = open("C:/TestPython/DWH_SQL_Server/DWH/logs/LogReportInpatientHIS.txt","w")
t0 = time.time()

conn_his_live = db_connection.create_connection(db_connection.his_live)
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
                        and b.PayPlanID != 71 and a.OrderID NOT LIKE '00%'
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

query_order = f"""
                    SELECT 
                        e.patient_id PatientID,
                        e.admission_id AdmissionID,
						CAST(b.tariff as decimal(15,2)) as Tarif,
                        CAST(e.ttl_paid as decimal(30,2)) as TarifTotal,
                        CASE
							WHEN b.item_t = 'patient_dispense' then 'obat'
                            WHEN b.item_t = 'patient_location' then 'akomodasi'
                            WHEN d.eklaim_group_nm  IS NULL THEN 'prosedur_non_bedah'	
                            ELSE d.eklaim_group_nm 
                        END AS EklaimGroup,
                        CASE
                            WHEN e.payplan_id = 8 THEN 'pribadi'
                            ELSE 'perusahaan'
                        END AS Jaminan
                from xocp_his_billing e 
                left join xocp_his_billing_item b on e.billing_id = b.billing_id and b.item_t IN ('patient_order','patient_dispense','patient_location')
				left join xocp_his_patient_order a on a.order_id = b.order_id
                left join xocp_obj_group c on a.obj_id = c.obj_id 
                left join xocp_group_eklaim d on c.eklaim_group = d.eklaim_group_id 
                where e.patient_id IN {patientid} and e.admission_id IN {admissionid}"""

order = pd.read_sql_query(query_order,conn_his_live)
source = source.merge(order,how='left',on=['PatientID','AdmissionID'])
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



sys.stdout.close()
