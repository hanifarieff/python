import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pyodbc
import pandas as pd
import numpy as np
import time
import datetime as dt
date = dt.datetime.today()
import sys

sys.stdout = open("C:/TestPython/DWH_SQL_Server/Staging/logs/LogTransPatientOrderRoleEHR.txt","w")
t0 = time.time()

#bikin koneksi ke db
conn_ehr = db_connection.create_connection(db_connection.replika_ehr)
conn_ehr_live = db_connection.create_connection(db_connection.ehr_live)
conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)

source_order = pd.read_sql_query("""
                               SELECT
                                    OrderID,
                                    PayPlanID,
                                    OrderDate,
                                    CreatedDate,
                                    Flag
                                FROM staging_rscm.TransPatientOrder
                                WHERE 
                                -- CAST(CreatedDate as date) = '2023-11-22'
                                ((CAST(CreatedDate as date) >= CAST(DATEADD(DAY, -1, GETDATE()) as date) AND CAST(CreatedDate as date) <= CAST(GETDATE() as date))
                                OR
                                (CAST(NullifiedDate as date) >= CAST(DATEADD(DAY, -1, GETDATE()) as date) AND CAST(NullifiedDate as date) <= CAST(GETDATE() as date)))  
                                AND Flag = 1 
                                -- AND OrderID IN ('00200007344269','00200007344270')
                                """, conn_staging_sqlserver)
orderid = tuple(source_order["OrderID"].unique())
source_order['PayPlanID'] = source_order['PayPlanID'].astype('int64') 

query_order_role = f"""SELECT order_id as OrderID, payplan_id as PayPlanID,role_no as RoleNo, default_obj_id as DefaultObjID, CAST(tariff as decimal(15,2)) as TarifAmount, CAST(disc_tariff as decimal(15,2)) as Discount
                        FROM xocp_ehr_patient_order_role where order_id IN {orderid} """
source_order_role = pd.read_sql_query(query_order_role, conn_ehr_live)
print('ini dari role')
print(source_order_role)

source = source_order.merge(source_order_role, how='left',on=['OrderID','PayPlanID'])

print(source)
source['RoleNo'].fillna(0, inplace=True)
source['RoleNo'] = source['RoleNo'].astype('int64')
source['TarifAmount'].fillna(0.00, inplace=True)
source['TarifAmount'] = source['TarifAmount'].astype('float64')
source['Discount'].fillna(0.00, inplace=True)
source['Discount'] = source['Discount'].astype('float64')
source['Flag'] = source['Flag'].astype('int64')
source['DefaultObjID'].replace(np.nan, None, inplace=True)
# source.replace(np.nan,None, inplace=True)

new_order_columns = ['OrderID','RoleNo','DefaultObjID','TarifAmount','Discount','OrderDate','CreatedDate','Flag']
source = source.reindex(columns=new_order_columns)

print(source)
print(source.dtypes)

if source.empty:
    print('tidak ada data dari source')
else:
    # jika dari source cuma 1 row
    if len(source) == 1:        
        # ambil primary key dari source, ambil index ke 0
        orderid = source["OrderID"].values[0]
        roleno = source=["RoleNo"].values[0]

        # query buat narik data dari target lalu filter berdasarkan primary key
        query = f"SELECT TRIM(OrderID) AS OrderID,RoleNo,DefaultObjID,TarifAmount,Discount,OrderDate,CreatedDate,Flag FROM staging_rscm.TransPatientOrderRole where OrderID IN ({orderid}) AND RoleNo IN ({roleno}) AND Flag = 1 ORDER BY OrderID, RoleNo"
        target = pd.read_sql_query(query, conn_staging_sqlserver)
    else :
         # ambil primary key dari source, pake unique biar tidak duplicate
        orderid = tuple(source["OrderID"].unique())
        roleno = tuple(source["RoleNo"])

        # query buat narik data dari target lalu filter berdasarkan primary key
        query = f"SELECT TRIM(OrderID) AS OrderID,RoleNo,DefaultObjID,TarifAmount,Discount,OrderDate,CreatedDate,Flag FROM staging_rscm.TransPatientOrderRole where OrderID IN {orderid} AND RoleNo IN {roleno} AND Flag = 1 ORDER BY OrderID, RoleNo"
        target = pd.read_sql_query(query, conn_staging_sqlserver)

    # cek tipe data target
    print(target)
    print(target.dtypes)

    # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]
    # print(source.apply(tuple,1).isin(target.apply(tuple,1)))
    # print(source.iloc[:,[17,19]])
    # print(target.iloc[:,[17,19]])
    # print(source.iloc[:,0:4].apply(tuple,1))
    # print(target.iloc[:,0:4].apply(tuple,1))
    # print(source.iloc[:,3:5].apply(tuple,1))
    # print(target.iloc[:,3:5].apply(tuple,1))
    # print(source.iloc[:,5:7].apply(tuple,1))
    # print(target.iloc[:,5:7].apply(tuple,1))
    # print(source.iloc[:,6:10].apply(tuple,1))
    # print(target.iloc[:,6:10].apply(tuple,1))
    # print(source.iloc[:,9:13].apply(tuple,1))
    # print(target.iloc[:,9:13].apply(tuple,1))
    # print(source.iloc[:,12:16].apply(tuple,1))
    # print(target.iloc[:,12:16].apply(tuple,1))
    # print(source.iloc[:,14:19].apply(tuple,1))
    # print(target.iloc[:,14:19].apply(tuple,1))
    # print(source.iloc[:,18:22].apply(tuple,1))
    # print(target.iloc[:,18:22].apply(tuple,1))
    # print(source.iloc[:,21:26].apply(tuple,1))
    # print(target.iloc[:,21:26].apply(tuple,1))
    # ambil data yang update dari changes
    modified = changes[changes[['OrderID','RoleNo']].apply(tuple,1).isin(target[['OrderID','RoleNo']].apply(tuple,1))]
    total_row_upd = len(modified)
    text_upd = f'total row update : {total_row_upd}'
    print(text_upd)
    print(modified.iloc[:,0:9])

    # ambil data yang new dari changes
    inserted = changes[~changes[['OrderID','RoleNo']].apply(tuple,1).isin(target[['OrderID','RoleNo']].apply(tuple,1))]
    total_row_ins = len(inserted)
    text_ins = f'total row inserted : {total_row_ins}'
    print(text_ins)
    print(inserted.iloc[:,0:9])

    if modified.empty:
        today = dt.datetime.now()
        today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
        inserted['InsertDateStaging'] = today_convert
        inserted.to_sql('TransPatientOrderRole',schema='staging_rscm', con = conn_staging_sqlserver, if_exists = 'append', index=False)
        print('success insert all data without update')
    
    else:
        # buat fungsi untuk update data ke tabel target
        def updated_to_sql(df, table_name, key_1,key_2):
            list_col = []
            table=table_name
            pk_1 = key_1
            pk_2 = key_2
            temp_table = f'{table}_temporary_table'
            for col in df.columns:
                if col == pk_1 or col == pk_2:
                    continue
                list_col.append(f'r.{col} = t.{col}')
            df.to_sql(temp_table,schema='staging_rscm',con=conn_staging_sqlserver, if_exists='replace',index=False)
            update_stmt_1 = f'UPDATE r '
            update_stmt_2 = f'SET '
            update_stmt_3 = ", ".join(list_col)
            update_stmt_8 = f' , r.UpdateDateStaging = CONVERT(DATETIME2(0), GETDATE(),120)'
            update_stmt_4 = f' FROM staging_rscm.{table} r '
            update_stmt_5 = f'INNER JOIN (SELECT * FROM staging_rscm.{temp_table}) as t ON r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} '
            update_stmt_6 = f' WHERE r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2}'
            update_stmt_7 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_8 + update_stmt_4 + update_stmt_5 + update_stmt_6 +";"
            delete_stmt_1 = f'DROP TABLE staging_rscm.{temp_table}'
            print(update_stmt_7)
            conn_staging_sqlserver.execute(update_stmt_7)
            conn_staging_sqlserver.execute(delete_stmt_1)

        try:
            # update data
            updated_to_sql(modified, 'TransPatientOrderRole', 'OrderID','RoleNo')

            today = dt.datetime.now()
            today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
            inserted['InsertDateStaging'] = today_convert
            inserted.to_sql('TransPatientOrderRole', con=conn_staging_sqlserver, if_exists ='append',index=False)
            print('success update and insert all data')
        
        except Exception as e:
            print(e)

#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)

text=f'scheduler tanggal : {date}'
print(text)

conn_ehr.close()
db_connection.close_connection(conn_ehr)
db_connection.close_connection(conn_ehr_live)
db_connection.close_connection(conn_staging_sqlserver)
