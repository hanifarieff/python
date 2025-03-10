import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pandas as pd
import pyodbc
import datetime as dt
date = dt.datetime.today()
import time


# bikin koneksi ke db
conn_his_live = db_connection.create_connection(db_connection.his_live)
conn_ehr_live = db_connection.create_connection(db_connection.ehr_live)
conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)


source = pd.read_sql_query(""" 
                        select order_id,patient_id,billing_id from xocp_his_patient_order
                        where ordered_dttm >= '2024-01-01 00:00:00' and ordered_dttm <= '2024-01-31 23:59:59' 
                        and billing_paid = '1'
                        -- and order_id IN ('150944105','150937960')
                        """, conn_his_live)
print('ini source')
print(source)

order_id = tuple(source["order_id"].unique())

def remove_comma(x):
    if len(x) ==1:
        return f"{x[0]}"
    else:
        return x

order_id = remove_comma(order_id)
get_ehr = f""" SELECT order_id FROM xocp_dm_tindakankencana
                WHERE order_id IN {order_id} """ 
ehr = pd.read_sql_query(get_ehr, conn_ehr_live)
print(ehr)


# # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
changes = source[~source.apply(tuple,1).isin(ehr.apply(tuple,1))]

# # ambil data yang update
# modified = changes[changes[['sep_no']].apply(tuple,1).isin(target[['sep_no']].apply(tuple,1))]
# total_row_upd = len(modified)
# text_upd = f'total row update : {total_row_upd}'
# print(text_upd)
# print(modified)

#ambil data yang baru
inserted = changes[~changes[['order_id']].apply(tuple,1).isin(ehr[['order_id']].apply(tuple,1))]
total_row_ins = len(inserted)
text_ins = f'total row inserted : {total_row_ins}'
print(text_ins)
print(inserted)

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
