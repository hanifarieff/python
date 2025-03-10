import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import sys
import time
import datetime as dt
date = dt.datetime.today()

sys.stdout = open("C:/TestPython/DWH_SQL_Server/DWH/logs/LogTesPostgres.txt","w")
t0 = time.time()

conn_transmedic = db_connection.create_connection(db_connection.transmedic)

source = pd.read_sql_query("""select * from pasiendaftar_t
where nocmfk = '187946' """,conn_transmedic)

source.drop('norec',axis=1,inplace=True)

source['namalengkapambilpasien'] = 'Jojo'
source['dikunjungi'] = 'ujicoba'
source['bahasa'] = 'indonesia'
source['keteranganasalrujukan'] = 'penyakit sulit diobati'

source = pd.concat([source] * 100000,ignore_index=True)
print(source.shape)

try:
    source.to_sql('pasiendaftar_t_copy1',con=conn_transmedic, if_exists ='append',index=False,chunksize=1000)
except Exception as e:
    print(e)

print('success insert')
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

text=f'scheduler tanggal : {date}'
print(text)

sys.stdout.close()
db_connection.close_connection(conn_transmedic)