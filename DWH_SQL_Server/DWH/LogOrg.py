import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import pandas as pd
import pyodbc
import datetime as dt
date = dt.datetime.today()
import time
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# bikin log ke file
sys.stdout = open("C:/TestPython/DWH_SQL_Server/DWH/logs/LogRowOrganization.txt","w")
text=f'scheduler tanggal : {date}'
print(text)
t0 = time.time()

# bikin koneksi ke db
conn_ehr_live = db_connection.create_connection(db_connection.ehr_live)
conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)
conn_dwh_sqlserver = db_connection.create_connection(db_connection.dwh_sqlserver)

# source dari ehr live
query = f""" SELECT 
                DATE(NOW()) ExtractDate,
                COUNT(*) TotalRowOrgLive
            FROM xocp_orgs
            """
source = pd.read_sql_query(query, conn_ehr_live)
print('ini source dari live')
print(source)

query_dwh = f"""
            select 
            CAST(GETDATE() AS DATE) AS ExtractDate,
            COUNT(*) AS TotalRowOrgDWH
            from dwhrscm_talend.DimOrganization
            where SCDActive ='1'
            and ChildOrganizationID != '99999'    
            """
source_dwh = pd.read_sql_query(query_dwh,conn_dwh_sqlserver)
print(source_dwh)


# joinkan antara data dari mpi dan dwh
source = source.merge(source_dwh, on='ExtractDate', how='inner')
print(source)


# Check for mismatched values
if source['TotalRowOrgLive'][0] != source['TotalRowOrgDWH'][0]:
    # Email Details
    sender_email = "lawkiddd2806@gmail.com"  # Replace with your email
    receiver_email = "lawkiddd2806@gmail.com"  # Replace with receiver's email
    password = "xoknfjvawuxitjvz"  # Replace with your app-specific password

    # Create the email message
    subject = "Alert: Data Table ORG Mismatch"
    body = (
        f"Dear Hanif,\n\n"
        f"A mismatch has been detected in the patient data:\n"
        f"TotalRowOrgLive: {source['TotalRowOrgLive'][0]}\n"
        f"TotalRowOrgDWH: {source['TotalRowOrgDWH'][0]}\n\n"
        f"Please investigate the issue.\n\n"
        f"Best regards,\nYour Monitoring Script"
    )
    
    # Set up the email server and send the email
    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = receiver_email
    message['Subject'] = subject
    message.attach(MIMEText(body, 'plain'))

    try:
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, message.as_string())
        print("Alert email sent successfully!")
    except Exception as e:
        print(f"Failed to send email: {e}")
else:
    print("No mismatch detected.")

source['ExtractDate'] = pd.to_datetime(source.ExtractDate, format='%Y-%m-%d')
source['ExtractDate'] = source['ExtractDate'].dt.strftime('%Y-%m-%d')

if source.empty:
    print('tidak ada data dari source')
else:
    extract_date = tuple(source["ExtractDate"].unique())

    # bikin function remove comma jika ada variable yang isinya cuma 1, variable ini akan di pakai IN CLAUSE di query target
    def remove_comma(x):
        if len(x) == 1:
            return str(x).replace(',','')
        else:
            return x
    
    extract_date = remove_comma(extract_date)

    query = f'SELECT ExtractDate,TotalRowOrgLive, TotalRowOrgDWH from dwhrscm_talend.LogOrganization where ExtractDate IN {extract_date} order by ExtractDate'
    target = pd.read_sql_query(query, conn_dwh_sqlserver)
    # print(target['ExtractDate'])
    target['ExtractDate'] = pd.to_datetime(target.ExtractDate, format='%Y-%m-%d',errors='coerce')
    target['ExtractDate'] = target['ExtractDate'].dt.strftime('%Y-%m-%d')
    print('ini target')
    print(target)

    # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
    changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

    # ambil data yang update dari changes
    modified = changes[changes[['ExtractDate']].apply(tuple,1).isin(target[['ExtractDate']].apply(tuple,1))]
    total_row_upd = len(modified)
    text_upd = f'total row update : {total_row_upd}'
    print(text_upd)
    print(modified)

    # ambil data yang new dari changes
    inserted = changes[~changes[['ExtractDate']].apply(tuple,1).isin(target[['ExtractDate']].apply(tuple,1))]
    total_row_ins = len(inserted)
    text_ins = f'total row inserted : {total_row_ins}'
    print(text_ins)
    print(inserted)

    if modified.empty:
        if inserted.empty:
            print('there is no data updated and inserted')
        else:
            inserted.to_sql('LogOrganization', schema='dwhrscm_talend', con = conn_dwh_sqlserver, if_exists = 'append', index=False)
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
            df.to_sql(temp_table,schema = 'dwhrscm_talend',con=conn_dwh_sqlserver, if_exists='replace',index=False)
            update_stmt_1 = f'UPDATE r '
            update_stmt_2 = f'SET '
            update_stmt_3 = ", ".join(list_col)
            update_stmt_4 = f' FROM dwhrscm_talend.{table} r '
            update_stmt_5 = f'INNER JOIN (SELECT * FROM dwhrscm_talend.{temp_table}) as t ON r.{pk_1} = t.{pk_1} '
            update_stmt_6 = f'WHERE r.{pk_1} = t.{pk_1} '
            update_stmt_7 = update_stmt_1 + update_stmt_2 + update_stmt_3  + update_stmt_4 + update_stmt_5 + update_stmt_6 +";"
            delete_stmt_1 = f'DROP TABLE dwhrscm_talend.{temp_table}'
            print(update_stmt_7)
            conn_dwh_sqlserver.execute(update_stmt_7)
            conn_dwh_sqlserver.execute(delete_stmt_1)

        try:
            # update data
            updated_to_sql(modified, 'LogOrganization', 'ExtractDate')

            # insert data baru
            
            inserted.to_sql('LogOrganization', schema='dwhrscm_talend', con=conn_dwh_sqlserver, if_exists ='append',index=False)
            print('success update and insert all data')
        
        except Exception as e:
            print(e)

t1 = time.time()
total=t1-t0
print(total)

text= f'scheduler tanggal : {date}'
print(text)
sys.stdout.close()

db_connection.close_connection(conn_ehr_live)
db_connection.close_connection(conn_staging_sqlserver)
db_connection.close_connection(conn_dwh_sqlserver)
sys.stdout.close()