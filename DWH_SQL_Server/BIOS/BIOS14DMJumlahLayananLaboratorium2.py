import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pandas as pd
import numpy as np
import time
import datetime as dt
date = dt.datetime.today()

# bikin log ke file
sys.stdout = open("C:/TestPython/DWH_SQL_Server/BIOS/logs/LogBIOS14DMJumlahLayananLaboratorium2.txt","w")
t0 = time.time()

# bikin koneksi ke db
conn_dwh_sqlserver = db_connection.create_connection(db_connection.dwh_sqlserver)
conn_bios_sqlserver = db_connection.create_connection(db_connection.bios_sqlserver)

source = pd.read_sql_query(""" SELECT 
                                    TglTransaksi,
                                    NamaLayanan,
                                    Jumlah
                                FROM dwhrscm_talend.[14DMJumlahLayananLaboratorium2]
                                WHERE 
                                TglTransaksi >= CAST(DATEADD(DAY, -10, GETDATE()) as date)
                                AND TglTransaksi <= CAST(GETDATE() as date)
                                -- TglTransaksi >= '2023-08-16' AND TglTransaksi <= '2023-08-23'
                                -- AND NamaLayanan = 'GLUKOSA RAPID (POCT)'
                                ORDER BY TglTransaksi
                            """, conn_dwh_sqlserver)
print(source)
print(source.dtypes)
if source.empty:
    print('tidak ada data dari source')
else:
    # jika dari source cuma 1 row
    if len(source) == 1:        
        # ambil primary key dari source, ambil index ke 0
        source['TglTransaksi'] = pd.to_datetime(source.TglTransaksi, format='%Y-%m-%d')
        source['TglTransaksi'] = source['TglTransaksi'].dt.strftime('%Y-%m-%d')
        tgltransaksi = source["TglTransaksi"].values[0]
        namalayanan = source["NamaLayanan"].values[0]
        # print(tgltransaksi)
        
        #narik data dari target
        query = f"SELECT TglTransaksi, NamaLayanan,Jumlah FROM dwhrscm_talend.BIOS14DMJumlahLayananLaboratorium2 WHERE TglTransaksi IN ('{tgltransaksi}') AND NamaLayanan IN ('{namalayanan}') ORDER BY TglTransaksi"
        target = pd.read_sql(query,conn_bios_sqlserver)
        target['TglTransaksi'] = pd.to_datetime(target.TglTransaksi,format='%Y-%m-%d')
        target['TglTransaksi'] = target['TglTransaksi'].dt.strftime('%Y-%m-%d')

    else:
        # ambil primary key dari source, pake unique biar tidak duplicate
        source['TglTransaksi'] = pd.to_datetime(source.TglTransaksi, format='%Y-%m-%d')
        source['TglTransaksi'] = source['TglTransaksi'].dt.strftime('%Y-%m-%d')
        tgltransaksi=tuple(source['TglTransaksi'])
        namalayanan=tuple(source['NamaLayanan'].unique())
        # print(tgltransaksi)

        query = f'SELECT TglTransaksi, NamaLayanan,Jumlah FROM dwhrscm_talend.BIOS14DMJumlahLayananLaboratorium2 WHERE TglTransaksi IN {tgltransaksi} AND NamaLayanan IN {namalayanan} ORDER BY TglTransaksi'
        target = pd.read_sql(query,conn_bios_sqlserver)
        target['TglTransaksi'] = pd.to_datetime(target.TglTransaksi,format='%Y-%m-%d')
        target['TglTransaksi'] = target['TglTransaksi'].dt.strftime('%Y-%m-%d')

    print(target)

    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

    modified = changes[changes[['TglTransaksi','NamaLayanan']].apply(tuple,1).isin(target[['TglTransaksi','NamaLayanan']].apply(tuple,1))]
    total_row_upd = len(modified)
    text_upd = f'total row update : {total_row_upd}'
    print(text_upd)
    print(modified)

    # ambil data yang new dari changes
    inserted = changes[~changes[['TglTransaksi','NamaLayanan']].apply(tuple,1).isin(target[['TglTransaksi','NamaLayanan']].apply(tuple,1))]
    total_row_ins = len(inserted)
    text_ins = f'total row inserted : {total_row_ins}'
    print(text_ins)
    print(inserted)

    if modified.empty:
        if inserted.empty:
            print('tidak ada data yang berubah dan data baru')
        else:
            inserted['IsSent'] = 0
            inserted.to_sql('BIOS14DMJumlahLayananLaboratorium2', schema='dwhrscm_talend',con=conn_bios_sqlserver, if_exists='append',index=False)
            print('success insert all data without update')
    else:
        # buat fungsi untuk update data ke tabel target 
        def updated_to_sql(df, table_name,key_1,key_2):
            list_col = []
            table=table_name
            pk_1 = key_1
            pk_2 = key_2
            temp_table =f'{table}_temporary_table'
            for col in df.columns:
                if col == pk_1 or col == pk_2:
                    continue
                list_col.append(f'r.{col}=t.{col}')
            df.to_sql(temp_table,schema='dwhrscm_talend',con=conn_bios_sqlserver, if_exists='replace',index=False)
            update_stmt_1 = f'UPDATE r '
            update_stmt_2 = f'SET '
            update_stmt_3 = ", ".join(list_col)
            update_stmt_4 = f' , r.IsSent = 0'
            update_stmt_5 = f' , r.UpdateDateDWH = CONVERT(DATETIME2(0), GETDATE(),120)'
            update_stmt_6 = f' FROM dwhrscm_talend.{table} r '
            update_stmt_7 = f'INNER JOIN (SELECT * FROM dwhrscm_talend.{temp_table}) as t ON r.{pk_1} = t.{pk_1} and r.{pk_2} = t.{pk_2} '
            update_stmt_8 = f' WHERE r.{pk_1} = t.{pk_1} and r.{pk_2} = t.{pk_2} '
            update_stmt_9 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_4 + update_stmt_5 + update_stmt_6 + update_stmt_7 + update_stmt_8 + ";"
            delete_stmt_1 = f'DROP TABLE dwhrscm_talend.{temp_table}'
            print(update_stmt_9)
            conn_bios_sqlserver.execute(update_stmt_9)
            conn_bios_sqlserver.execute(delete_stmt_1)
        
        try:
            #update data
            updated_to_sql(modified, 'BIOS14DMJumlahLayananLaboratorium2', 'TglTransaksi','NamaLayanan')

            #insert data baru
            inserted['IsSent'] = 0
            inserted.to_sql('BIOS14DMJumlahLayananLaboratorium2', schema='dwhrscm_talend',con=conn_bios_sqlserver, if_exists='append',index=False)
            print('success update and insert all data')
        except Exception as e:
            print(e)

#hitung kecepatan eksekusi program
t1 = time.time()
total=t1-t0
print(total)

text=f'scheduler tanggal : {date}'
print(text)

db_connection.close_connection(conn_dwh_sqlserver)
db_connection.close_connection(conn_bios_sqlserver)