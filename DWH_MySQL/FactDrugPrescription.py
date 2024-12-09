from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine as ce
import pandas as pd
import pyodbc

mysql = ce("mysql://hanif-ppi:hanif2022@172.16.19.11/ehr")
dwh = ce("mysql://hanif-ppi:hanif2022@172.16.5.33/dwhrscm_talend")
sql_server = ce('mssql+pyodbc://andhi-ppi:Andhi2022!@172.16.19.36/DWH_RSCM?driver=SQL+Server+Native+Client+11.0', fast_executemany =True)
try :
    con_mysql = mysql.connect()
    con_dwh = dwh.connect()
    con_mssql = sql_server.connect()
    print('successfully connect to all database')
    
except SQLAlchemyError as e:
    error=str(e.__dict__['orig'])
    print(error)

# tarik data dari source 
source = pd.read_sql_query(""" SELECT
                                    a.prescription_id as PrescriptionID, -- char(15)
                                    b.item_id as SequenceID, -- int(11)
                                    a.patient_id as PatientID, -- int(10)
                                    a.admission_id as AdmissionID, -- int(10)
                                    a.presc_org_id as PrescriptionOrgID,
                                    b.obj_id as DrugID, -- char(20)
                                    b.obj_qty as DrugQuantity, -- char(30)
                                    b.freq_cd as FrequencyCode, -- varchar(255)
                                    b.unit_cd as UnitCode, -- char(15)
                                    b.x_qty as DispenseQuantity, -- char(30)
                                    b.x_unit_cd as DispenseUnitCode, -- char(15)
                                    a.created_dttm as InsertDateApp
                                FROM
                                xocp_ehr_prescription AS a
                                inner JOIN xocp_ehr_prescription_item AS b ON a.prescription_id = b.prescription_id
                                where admission_id <> '0' and 
                                DATE(a.created_dttm) >= DATE(date_sub(now(), interval 1 DAY)) AND DATE(a.created_dttm) < DATE(now())
        """, con_mysql)
print(source.shape)

#Load source ke target
source.to_sql('FactDrugPrescription', con_dwh, if_exists = 'append', index=False)
source.to_sql('FactDrugPrescription',schema='dwhrscm_talend',con=con_mssql, if_exists='append',index=False)
print('successfully inserted')

con_mysql.close()
con_dwh.close()
