""" File ini untuk menarik data dari `admisi` ke `admisi_target` """ 
import sys
import time
import pandas as pd

sys.path.insert(1, 'E://WORK_RSCM//Python//Connection')
import db_connection


def get_connections():
    """ membuat koneksi ke database yang dibutuhkan """
    conn_mysql = db_connection.create_connection(db_connection.mysql_local)
    conn_sqlserver = db_connection.create_connection(db_connection.mssql_local)
    return conn_mysql, conn_sqlserver

def get_source_data(conn_mysql):
    """ Extract data dari source """
    query_source = f""" SELECT * FROM tes_admisi """
    return pd.read_sql_query(query_source, conn_mysql)

def fetch_data_from_sqlserver(source, conn_sqlserver):
    """ ambil data dari tabel lain untuk join ke dataframe source """
    medical_no_list = []
    diagnose_no_list = []

    for index,row in source.iterrows():
        patient_id = row['patient_id']
        admission_id = row['admission_id']

        query_medical_no = f"""
                            SELECT patient_id,medical_no
                            FROM dbo.cihuy
                            WHERE patient_id = {patient_id}
                        """
        medical_no = pd.read_sql_query(query_medical_no,conn_sqlserver)

        if not medical_no.empty:
            medical_no_list.append(medical_no.iloc[0]['medical_no'])
        else:
            medical_no_list.append(None)

        query_diagnose = f""" 
                            SELECT patient_id,admission_id, diagnose
                            FROM dbo.diagnose
                            WHERE patient_id = {patient_id} and admission_id = {admission_id}
                        """
        diagnose = pd.read_sql_query(query_diagnose,conn_sqlserver)

        if not diagnose.empty:
            diagnose_no_list.append(diagnose.iloc[0]['diagnose'])
        else:
            diagnose_no_list.append(None)

    source['medical_no'] = medical_no_list
    source['diagnose'] = diagnose_no_list
    
    return source

def fetch_target_data(source, conn_sqlserver):
    """ ambil data dari database target """
    target = pd.DataFrame(columns=['patient_id', 'admission_id', 'org_id',
                                   'status', 'caramasuk', 'medical_no', 'diagnose'])
    
    query_target = """SELECT * 
                      FROM dbo.tes_admisi_target 
                      WHERE patient_id = ? AND admission_id = ?
                      ORDER BY patient_id"""
    
    with conn_sqlserver.connect() as conn:
        for index, row in source.iterrows():
            pk_values = (row['patient_id'], row['admission_id'])
            
            # jalankan query dengan parameter pk_values
            results = conn.execute(query_target, pk_values).fetchall()
            
            # jika ada hasilnya maka masukkan ke dataframe target
            if results:
                result_df = pd.DataFrame.from_records(results, columns=target.columns)
                target = pd.concat([target, result_df], ignore_index=True)
            else:
                print('The results are empty for patient_id:', row['patient_id'], 
                      'and admission_id:', row['admission_id'])
    
    return target
    
def detect_changes(source, target):
    """ deteksi perubahan antara dataframe `source` dan `target` """
    change = source[~source.apply(tuple, 1).isin(target.apply(tuple, 1))]
    
    modified = change[change[['patient_id', 'admission_id']].apply(tuple, 1).isin(target[['patient_id', 'admission_id']].apply(tuple, 1))]
    inserted = change[~change[['patient_id', 'admission_id']].apply(tuple, 1).isin(target[['patient_id', 'admission_id']].apply(tuple, 1))]

    return modified, inserted

def updated_data(df, table_name, key_1, key_2, conn_sqlserver):
    """ update data di database target """
    if not df.empty:  
        a = [f't.{col} = s.{col}' for col in df.columns if col != key_1 and col != key_2]
        temp_table = f'{table_name}_temporary_table'
        
        # Upload the temp table to SQL Server
        df.to_sql(temp_table, schema='dbo', con=conn_sqlserver, if_exists='replace', index=False)

        update_stmt = (
            f'UPDATE t SET ' + ", ".join(a) +
            f' FROM dbo.{table_name} t '
            f'INNER JOIN (SELECT * FROM dbo.{temp_table}) AS s ON t.{key_1} = s.{key_1} AND t.{key_2} = s.{key_2} '
            f'WHERE t.{key_1} = s.{key_1} AND t.{key_2} = s.{key_2};'
        )
        delete_stmt = f'DROP TABLE {temp_table};'

        with conn_sqlserver.begin() as transaction:
            # Execute update and delete temp table
            conn_sqlserver.execute(update_stmt)
            conn_sqlserver.execute(delete_stmt)
            print('\nData Success Updated')
    else:
        print('\nTidak ada data yang berubah')

def inserted_data(inserted, conn_sqlserver):
    """ insert data di database target """
    if not inserted.empty:
        with conn_sqlserver.begin() as transaction:
            inserted.to_sql('tes_admisi_target', schema='dbo', con=conn_sqlserver, if_exists='append', index=False)
            print('Data Success Inserted')
    else:
        print('Tidak ada data yang baru')

def main():
    """ Fungsi utama untuk menjalankan semua proses """
    conn_mysql, conn_sqlserver = get_connections()
    try:
        source = get_source_data(conn_mysql)
        """ extract data dari source"""
        print("Source Data:")
        print(source)

        # extract data dari tabel lain dan gabung ke dataframe source
        source = fetch_data_from_sqlserver(source,conn_sqlserver)
        print("Setelah join dengan tabel lain:")
        print(source)

        # ambil data dari database target
        target = fetch_target_data(source, conn_sqlserver)

        # Deteksi perubahan (buat dapetin modified dan inserted)
        modified, inserted = detect_changes(source, target)
        print("Changes Detected:")
        print("Modified Data:")
        print(modified)
        print("Inserted Data:")
        print(inserted)
        
        # update data
        updated_data(modified, 'tes_admisi_target', 'patient_id', 'admission_id', conn_sqlserver)
      
        # insert data
        inserted_data(inserted,conn_sqlserver)

    finally:
        # close koneksi database
        db_connection.close_connection(conn_mysql)
        db_connection.close_connection(conn_sqlserver)
        print('\nConnection closed successfully!')

# Run the main process
if __name__ == "__main__":
    main()
