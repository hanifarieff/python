from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine as ce
import pyodbc
import pandas as pd

mysql = ce("mysql://hanif-ppi:hanif2022@172.16.19.11/ehr")
dwh = ce("mysql://hanif-ppi:hanif2022@172.16.5.33/dwhrscm_talend")
sql_server = ce('mssql+pyodbc://andhi-ppi:Andhi2022!@172.16.19.36/DWH_RSCM?driver=SQL+Server+Native+Client+11.0', fast_executemany=True) 
try :
    con_mysql = mysql.connect()
    con_dwh = dwh.connect()
    con_mssql = sql_server.connect()
    print('successfully connect to all database')
    
except SQLAlchemyError as e:
    error=str(e.__dict__['orig'])
    print(error)

source = pd.read_sql_query(""" SELECT
                                a.prescription_id as PrescriptionID,
                                b.item_id as SequenceID,
                                a.patient_id as PatientID, 
                                a.admission_id as AdmissionID,
                                b.obj_id as DrugID, 
                                b.obj_qty DrugQuantity, 
                                b.freq_cd as FrequencyCode,  
                                b.unit_cd as UnitCode, 
                                b.x_qty as DispenseQuantity, 
                                b.x_unit_cd as DispenseUnitCode, 
                                a.status_cd as DispenseStatus,
                                a.created_dttm as InsertDateApp
                                FROM
                                xocp_ehr_prescription_x AS a
                                inner JOIN xocp_ehr_prescription_x_item AS b ON a.prescription_id = b.prescription_id
                                WHERE
                                DATE(a.created_dttm) >= DATE(date_sub(now(), interval 1 DAY)) AND DATE(a.created_dttm) < DATE(now()) and a.status_cd = 'final'
                                order by created_dttm 
                                
        """, con_mysql)
source
print(source.shape)

source.to_sql('FactDrugDispense', con_dwh, if_exists = 'append', index=False)
source.to_sql('FactDrugDispense',schema='dwhrscm_talend',con=con_mssql, if_exists='append',index=False)
print('successfully inserted')

con_mysql.close()
con_dwh.close()