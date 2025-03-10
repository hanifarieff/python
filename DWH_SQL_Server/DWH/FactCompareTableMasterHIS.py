import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pyodbc
import pandas as pd
import time
import datetime as dt
date = dt.datetime.today()

import psutil
pid = psutil.Process()

# bikin log ke file
sys.stdout = open("C:/TestPython/DWH_SQL_Server/DWH/logs/LogFactCompareTableMasterHIS.txt","w")
t0 = time.time()

# Start monitoring CPU usage 
cpu_start = psutil.cpu_percent() 

# connection database
conn_ehr_live = db_connection.create_connection(db_connection.ehr_live)
conn_his_live = db_connection.create_connection(db_connection.his_live)
conn_dwh_sqlserver = db_connection.create_connection(db_connection.dwh_sqlserver)

# buat fungsi untuk dengan parameter *args, parameter isinya list list tabel ehr live

def get_data_his_live(*list_table):

    dataframes = []

    for index, val in enumerate(list_table, start=1):
        try:
            query = (f"SELECT COUNT(*) AS RowTotal FROM {val}")
            data = pd.read_sql_query(query, conn_his_live)
            data.insert(0,'ID',index)
            data.insert(1,'TableName',val)
            dataframes.append(data)
        except Exception as e:
            print(e)
    source_his = pd.concat(dataframes, ignore_index=True)
    return source_his
    print('\n')

# jalakan fungsi ehr live dan his live, masukkan ke variabel ehr dan his untuk nanti digabungkan jadi 1 dataframe
source = get_data_his_live('xocp_his_patient_admission','xocp_his_patient_location','xocp_orgs','xocp_his_patient_act','xocp_his_patient','xocp_his_obj_payplan','xocp_his_formulir_darah','xocp_his_patient_order')
# xocp_ehr_patient_act','xocp_ehr_patient','xocp_ehr_payplan','xocp_ehr_company'
# xocp_his_patient_act','xocp_his_formulir_darah'
# gabungkan variabel ehr dan his jadi 1 dataframe
# source = pd.concat([ehr, his], ignore_index=True)
source['ID'] = source.index + 9 # Setting 'ID' as 1-based index
print(source)

id = tuple(source['ID'])

query = f"SELECT ID, TableName, RowTotal FROM dwhrscm_talend.FactRowNumberMaster WHERE ID IN {id}"
target = pd.read_sql_query(query, conn_dwh_sqlserver)

changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

modified = changes[changes[["ID"]].apply(tuple,1).isin(target[["ID"]].apply(tuple,1))]
total_row_upd = len(modified)
text_upd = f'total row update master : {total_row_upd}'
print(text_upd)
print(modified)
print('\n')

inserted = changes[~changes[["ID"]].apply(tuple,1).isin(target[["ID"]].apply(tuple,1))]
total_row_upd = len(inserted)
text_upd = f'total row insert master : {total_row_upd}'
print(text_upd)
print(inserted)

if modified.empty:
        # bikin tanggal sekarang buat kolom InsertDateStaging
    today = dt.datetime.now()
    today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
    inserted['InsertedDateDWH'] = today_convert
    inserted.to_sql('FactRowNumberMaster', schema = 'dwhrscm_talend',con = conn_dwh_sqlserver, if_exists = 'append', index=False)
    print('success insert all data without update')

else:
    # buat fungsi untuk update data ke tabel target
    def updated_to_sql(df, table_name, key_1):
        list_col = []
        table=table_name
        pk_1 = key_1
        temp_table = f'{table}_temporary_table'
        for col in df.columns:
            if col == pk_1 :
                continue
            list_col.append(f'r.{col} = t.{col}')
        df.to_sql(temp_table,schema='dwhrscm_talend',con=conn_dwh_sqlserver, if_exists='replace',index=False)
        update_stmt_1 = f'UPDATE r '
        update_stmt_2 = f'SET '
        update_stmt_3 = ", ".join(list_col)
        update_stmt_8 = f' , r.UpdatedDateDWH = CONVERT(DATETIME2(0), GETDATE(),120)'
        update_stmt_4 = f' FROM dwhrscm_talend.{table} r '
        update_stmt_5 = f'INNER JOIN (SELECT * FROM dwhrscm_talend.{temp_table}) as t ON r.{pk_1} = t.{pk_1} '
        update_stmt_6 = f' WHERE r.{pk_1} = t.{pk_1} '
        update_stmt_7 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_8 + update_stmt_4 + update_stmt_5 + update_stmt_6 +";"
        delete_stmt_1 = f'DROP TABLE dwhrscm_talend.{temp_table}'
        print(update_stmt_7)
        conn_dwh_sqlserver.execute(update_stmt_7)
        conn_dwh_sqlserver.execute(delete_stmt_1)

    try:
        # update data
        updated_to_sql(modified, 'FactRowNumberMaster', 'ID')

        # Insert data baru
        today = dt.datetime.now()
        today_convert = today.strftime("%Y-%m-%d %H:%M:%S")
        inserted['InsertedDateDWH'] = today_convert
        inserted.to_sql('FactRowNumberMaster', schema='dwhrscm_talend',con=conn_dwh_sqlserver, if_exists ='append',index=False)
        print('success update and insert all data master\n')
    
    except Exception as e:
        print(e)
 
# Stop monitoring CPU usage 
cpu_end = psutil.cpu_percent() 
 
# Calculate the difference between the start and end points 
cpu_diff = cpu_end - cpu_start 
print("CPU usage:", cpu_diff, "%")

# check RAM usage
mem = psutil.Process().memory_info().rss / (1024 * 1024)
print(mem) 

# check berapa detik program running
t1 = time.time()
total=t1-t0
print(total)

text=f'scheduler tanggal : {date}'
print(text)

db_connection.close_connection(conn_ehr_live)
db_connection.close_connection(conn_dwh_sqlserver)
sys.stdout.close()