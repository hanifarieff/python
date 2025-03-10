from sqlalchemy import create_engine as ce
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import sys

sys.stdout = open("C:/TestPython/DWH_SQL_Server/DWH/logs/LogFactRevenuePerUnit.txt","w")

dwh_sqlserver = ce('mssql+pyodbc://dev-ppi:D3vpp122!@172.16.19.36/DWH_RSCM?driver=SQL+Server+Native+Client+11.0', fast_executemany=True)
try:
    conn_dwh_sqlserver = dwh_sqlserver.connect()
    print('successfully connect db')
except SQLAlchemyError as e:
    error=str(e.__dict__['orig'])
    print(error)

source = pd.read_excel(r'E:\Revenue\Template Pendapatan Per Unit - DWH.xlsx',sheet_name='DWH')
source.columns=['Period', 'UnitID','UnitName','Revenue']
source['Period'] = pd.to_datetime(source.Period, format='%Y-%m-%d')
source['Period'] = source['Period'].dt.strftime('%Y-%m-%d')
print(source)
print(source.dtypes)
# try:
#     source.to_sql('FactRevenuePerUnit',con=conn_dwh,if_exists='append',index=False)
#     print('success')
# except Exception as e:
#     print(e)

# ambil primary key dari source
period = tuple(source["Period"])
unit = tuple(source["UnitID"])
# # print(period)
# query buat narik data dari target lalu filter berdasarkan primary key
query = f'SELECT * FROM dwhrscm_talend.FactRevenuePerUnit where Period IN {period} AND UnitID IN {unit} order by Period asc,UnitID asc'
target = pd.read_sql_query (query, conn_dwh_sqlserver)
target['Period'] = pd.to_datetime(target.Period, format='%Y-%m-%d')
target['Period'] = target['Period'].dt.strftime('%Y-%m-%d')
# target['Revenue'] = target['Revenue'].astype(str) 
print(target)
print(target.dtypes)

# ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

# print(source.iloc[:,0:1].apply(tuple,1))
# print(target.iloc[:,0:1].apply(tuple,1))
# print(source.iloc[:,1:2].apply(tuple,1))
# print(target.iloc[:,1:2].apply(tuple,1))
# print(source.iloc[:,2:3].apply(tuple,1))
# print(target.iloc[:,2:3].apply(tuple,1))
# print(source.iloc[:,3:4].apply(tuple,1))
# print(target.iloc[:,3:4].apply(tuple,1))

# data yang terupdate
modified = changes[changes[['Period','UnitID']].apply(tuple,1).isin(target[['Period','UnitID']].apply(tuple,1))]
print('ini update')
print(modified)
# data baru
inserted =  changes[~changes[['Period','UnitID']].apply(tuple,1).isin(target[['Period','UnitID']].apply(tuple,1))]
print('ini data baru')
print(inserted)

# buat function untuk update data
def updated_to_sql(df, table_name, key_1, key_2):
    a = []
    table = table_name
    pk_1 = key_1
    pk_2 = key_2
    temp_table = f'{table}_temporary_table'
    for col in df.columns:
        if col == pk_1 or col == pk_2:
            continue
        a.append(f't.{col} = s.{col}')
    df.to_sql(temp_table, schema = 'dwhrscm_talend',con=conn_dwh_sqlserver, if_exists = 'replace', index=False)
    update_stmt_1 = f'UPDATE t '
    update_stmt_2 = f'SET '
    update_stmt_3 = ", ".join(a)
    update_stmt_4 = f' FROM dwhrscm_talend.{table} t '
    update_stmt_5 = f' INNER JOIN (SELECT * FROM dwhrscm_talend.{temp_table}) AS s ON t.{pk_1} = s.{pk_1} AND t.{pk_2} = s.{pk_2} '
    update_stmt_6 = f' WHERE t.{pk_1} = s.{pk_1} AND t.{pk_2} = s.{pk_2}'
    update_stmt_7 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_4 + update_stmt_5 + update_stmt_6 + ";"
    delete_stmt_1 = f'DROP TABLE dwhrscm_talend.{temp_table} '
    print(update_stmt_7)
    print('\n')
    conn_dwh_sqlserver.execute(update_stmt_7)
    conn_dwh_sqlserver.execute(delete_stmt_1)
try: 
    
    # panggil function update untuk proses update data ke target
    updated_to_sql(modified, 'FactRevenuePerUnit', 'Period','UnitID')

    # insert data baru ke target
    inserted.to_sql('FactRevenuePerUnit', schema='dwhrscm_talend',con = conn_dwh_sqlserver, if_exists='append', index=False)
    print('successfully update and insert all data')
except Exception as e:
    print(e)

conn_dwh_sqlserver.close()
sys.stdout.close()