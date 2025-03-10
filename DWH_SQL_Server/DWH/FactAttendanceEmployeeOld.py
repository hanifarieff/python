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

sys.stdout = open("C:/TestPython/DWH_SQL_Server/DWH/logs/LogFactAttendanceEmployee.txt","w")
t0 = time.time()

conn_ehr = db_connection.create_connection(db_connection.replika_ehr)
conn_attendance = db_connection.create_connection(db_connection.attendance)
conn_siknet = db_connection.create_connection(db_connection.siknet)
conn_dwh_sqlserver= db_connection.create_connection(db_connection.dwh_sqlserver)

attendance_df = pd.read_sql_query(""" 
                        SELECT  
                            employee_id as EmployeeID,
                            -- employee_org_id AS EmployeeOrgID,
                            -- TRIM(employee_ext_id) as EmployeeNo,
                            -- employee_nm as EmployeeName,
                            attendance_date as AttendanceDate,
                            attendance_start as AttendanceStart,
                            attendance_stop as AttendanceStop
                        FROM xocp_his_employee_attendance
                        where 
                        employee_ext_id IN ('198010152005012002') AND
                        -- and attendance_date >= '2022-01-01' and attendance_date <= '2022-09-30'
                        employee_ext_id NOT IN ('') AND
                        (attendance_date >= '2024-07-01' and attendance_date <= '2024-07-23')
                        -- (attendance_date >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 60 DAY), "%%Y-%%m-%%d") and attendance_date <= DATE_FORMAT(NOW(), "%%Y-%%m-%%d"))
                        -- OR (updated_dttm >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), "%%Y-%%m-%%d") and updated_dttm <= DATE_FORMAT(NOW(), "%%Y-%%m-%%d"))
                        order by employee_nm,attendance_start 
                    """, conn_attendance) 

# convert kolom attendancedate dari string jadi tanggal
attendance_df['AttendanceDate'] = pd.to_datetime(attendance_df.AttendanceDate, format='%Y-%m-%d')
attendance_df['AttendanceDate'] = attendance_df['AttendanceDate'].dt.strftime('%Y-%m-%d')

employee_id = pd.read_sql_query(""" SELECT employee_id as EmployeeID,
                                    person_nm as EmployeeName,
                                    employee_org_id as EmployeeOrgID,employee_ext_id as EmployeeNo 
                                    FROM xocp_hrm_employee
                                    WHERE status_cd = 'active'""",conn_ehr)

attendance_df = attendance_df.merge(employee_id,how='left',on='EmployeeID')

# attendance_df = attendance_df[(attendance_df['EmployeeNo'] == '198409022012122001') |(attendance_df['EmployeeNo']=='196306091985032011')]
print(attendance_df)
siknet_df = pd.read_sql_query(""" 
                        SELECT 
                            TRIM(nip) as EmployeeNo, 
                            CAST(tglbuku as date) as AttendanceDate,
                            MIN(tglwktmasuk) as AttendanceStart, 
                            MIN(tglwktkeluar) as AttendanceStop 
                        from tbltransidp
                        where 
                        nip IN ('198010152005012002') AND
                        -- and tglwktmasuk >= '2023-08-01 00:00:00' and tglwktkeluar <= '2023-08-30 23:59:59'
                        nip NOT IN ('') AND
                        tglbuku >= '2024-07-01 00:00:00' and tglbuku <= '2024-07-23 23:59:59'
                        -- tglbuku >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 60 DAY), "%%Y-%%m-%%d 00:00:00") and tglbuku <= DATE_FORMAT(NOW(), "%%Y-%%m-%%d 23:59:59")
                        GROUP BY TRIM(nip),CAST(tglbuku as date)
                        ORDER BY nip,tglwktmasuk
                    """, conn_siknet)

# siknet_df = siknet_df[(siknet_df['EmployeeNo']=='198409022012122001') |(siknet_df['EmployeeNo']=='196306091985032011') ]

# replace EmployeeNo yang ada \r\n nya
siknet_df['EmployeeNo'] = siknet_df['EmployeeNo'].str.replace('\r\n', '', regex=True)

# convert kolom attendancedate dari string jadi tanggal
siknet_df['AttendanceDate'] = pd.to_datetime(siknet_df.AttendanceDate, format='%Y-%m-%d')
siknet_df['AttendanceDate'] = siknet_df['AttendanceDate'].dt.strftime('%Y-%m-%d')

print('dari siknet')
print(siknet_df)

merged_attendance = attendance_df.merge(siknet_df,how='outer', on=['EmployeeNo','AttendanceDate'], suffixes=('_attendance', '_siknet'))
merged_attendance.sort_values(by=['EmployeeID','AttendanceDate'])

# merged_attendance['EmployeeID'].ffill(inplace=True)
# merged_attendance['EmployeeOrgID'].ffill(inplace=True)
# merged_attendance['EmployeeName'].ffill(inplace=True)
# print(merged_attendance.iloc[:,0:6].sort_values(by=['AttendanceDate']))
# print(merged_attendance.sort_values(by=['AttendanceDate']))

# merged_attendance = attendance_df.join(siknet_df, on=['EmployeeNo','AttendanceDate'])
merged_attendance['AttendanceStart_attendance'] = pd.to_datetime(merged_attendance['AttendanceStart_attendance'], format='%H:%M:%S')
merged_attendance['AttendanceStart_siknet'] = pd.to_datetime(merged_attendance['AttendanceStart_siknet'], format='%H:%M:%S')

merged_attendance['AttendanceStop_attendance'] = pd.to_datetime(merged_attendance['AttendanceStop_attendance'], format='%H:%M:%S')
merged_attendance['AttendanceStop_siknet'] = pd.to_datetime(merged_attendance['AttendanceStop_siknet'], format='%H:%M:%S')

merged_attendance['AttendanceStart'] = merged_attendance[['AttendanceStart_attendance','AttendanceStart_siknet']].min(axis=1)
merged_attendance['AttendanceStop'] = merged_attendance[['AttendanceStop_attendance','AttendanceStop_siknet']].max(axis=1)

# print(merged_attendance.iloc[:,0:9].sort_values(by=['AttendanceDate']))

# Handle the case where 'AttendanceStart_siknet' contains a valid time while 'AttendanceStart_attendance' contains '00:00:00'
merged_attendance.loc[
    (merged_attendance['AttendanceStart_siknet'].dt.time != pd.Timestamp('00:00:00').time()) &
    (merged_attendance['AttendanceStart_attendance'].dt.time == pd.Timestamp('00:00:00').time()),
    'AttendanceStart'
] = merged_attendance['AttendanceStart_siknet']

# Handle the case where 'AttendanceStart_attendance' contains a valid time while 'AttendanceStart_siknet' contains '00:00:00'
merged_attendance.loc[
    (merged_attendance['AttendanceStart_siknet'].dt.time == pd.Timestamp('00:00:00').time()) &
    (merged_attendance['AttendanceStart_attendance'].dt.time != pd.Timestamp('00:00:00').time()),
    'AttendanceStart'
] = merged_attendance['AttendanceStart_attendance']

# Check if both times are '00:00:00' in either dataframe and set the result accordingly
merged_attendance.loc[
    (merged_attendance['AttendanceStart_siknet'].dt.time == pd.Timestamp('00:00:00').time()) &
    (merged_attendance['AttendanceStart_attendance'].dt.time == pd.Timestamp('00:00:00').time()),
    'AttendanceStart'
] = merged_attendance['AttendanceStart_siknet']

merged_attendance.loc[
    (merged_attendance['AttendanceStop_siknet'].dt.time != pd.Timestamp('00:00:00').time()) &
    (merged_attendance['AttendanceStop_attendance'].dt.time == pd.Timestamp('00:00:00').time()),
    'AttendanceStop'
] = merged_attendance['AttendanceStop_siknet']

merged_attendance['AttendanceStart'] = np.where((merged_attendance['AttendanceStart_attendance'].isna()) & (merged_attendance['AttendanceStart_siknet'].dt.time == pd.Timestamp('00:00:00').time()), merged_attendance['AttendanceStart_siknet'], merged_attendance['AttendanceStart'])

print('ini fix')
print(merged_attendance.iloc[:,0:8].sort_values(by=['EmployeeID','AttendanceDate']))

# Create flag for column IsSiknetStart
merged_attendance['IsSiknetStart'] = np.where(
    merged_attendance['AttendanceStart'] == merged_attendance['AttendanceStart_siknet'],
    1, # 1 for Siknet
    0  # 0 for Attendance
)

# Create flag for column IsSiknetStop
merged_attendance['IsSiknetStop'] = np.where(
    merged_attendance['AttendanceStop'] == merged_attendance['AttendanceStop_siknet'],
    1,  # 1 for Siknet
    0   # 0 for Attendance
)
# Drop column unnecessary after join
merged_attendance.drop(['AttendanceStart_attendance','AttendanceStart_siknet','AttendanceStop_siknet', 'AttendanceStop_attendance'], axis=1, inplace=True)

# Re-ordered columns
new_order_columns = ('EmployeeID','EmployeeOrgID','EmployeeNo','EmployeeName','AttendanceDate','AttendanceStart','IsSiknetStart','AttendanceStop','IsSiknetStop')
merged_attendance = merged_attendance.reindex(columns=new_order_columns)

# merged_attendance['AttendanceDate']=pd.to_datetime(merged_attendance['AttendanceDate'])
merged_attendance.drop_duplicates(inplace=True)

# Convert Kolom AttendanceStart dan AttendanceStop misalnya ada yang gak absen, diambil kolom AttendanceDate ditambah jam 00:00:00
merged_attendance['AttendanceStop'].fillna(pd.to_datetime(merged_attendance['AttendanceDate'] + ' 00:00:00', format="%Y-%m-%d %H:%M:%S"),inplace=True)
merged_attendance['AttendanceStart'].fillna(pd.to_datetime(merged_attendance['AttendanceDate'] + ' 00:00:00', format="%Y-%m-%d %H:%M:%S"),inplace=True)
# print('lagi')
# print(merged_attendance.iloc[:,4:9].sort_values(by=['AttendanceDate']))


print('setelah ditambah hari libur')
# Remove rows based on the condition
# merged_attendance = merged_attendance[~condition]
print(merged_attendance.iloc[:,[0,3,4,5,7]].sort_values(by=['AttendanceDate']))


# Kondisi jika tanggal absen dateng dan pulang jamnya sama contoh 08:00:00 dan keduanya dari siknet, maka jam pulang ubah jadi 00:00:00
condition = (merged_attendance["IsSiknetStart"] == 1) & (merged_attendance["IsSiknetStop"] == 1) & (merged_attendance["AttendanceStart"] == merged_attendance["AttendanceStop"])
merged_attendance.loc[condition, "AttendanceStop"] = merged_attendance.loc[condition, "AttendanceStop"].dt.normalize()
print(merged_attendance.iloc[:,[0,3,4,5,7]].sort_values(by=['AttendanceDate']))

# # Isi value EmployeeID dan EmployeeOrgID yang null dengan value sebelumnya
# print(merged_attendance[merged_attendance['EmployeeID'].isna()])
# merged_attendance['EmployeeID']=merged_attendance['EmployeeID'].ffill()
# merged_attendance['EmployeeOrgID'] = merged_attendance['EmployeeOrgID'].ffill() 
# print(merged_attendance[merged_attendance['EmployeeID'] == 11413])

# # Join lagi ke EmployeeID buat ngambil kolom EmployeeNo menyesuaikan yang terbaru, karena yg dari siknet biasanya EmployeeNo yg lama.
# merged_attendance = merged_attendance.merge(employee_id,how='inner',on='EmployeeID')
# print(merged_attendance[merged_attendance['EmployeeID'] == 11413])
# merged_attendance['EmployeeNo_x'] = merged_attendance['EmployeeNo_y']
# merged_attendance.rename(columns={'EmployeeNo_x':'EmployeeNo'},inplace=True)
# merged_attendance.drop(columns='EmployeeNo_y',inplace=True)

# print('cek employeeno baru')
# print(merged_attendance.iloc[:,0:9].sort_values(by=['EmployeeID','AttendanceDate']))
# print(merged_attendance[merged_attendance['EmployeeID'] == 11413])

# buat ngisi EmployeeID, EmployeeOrgID dan EmployeeName yang kosong mengikuti kolom yang diatasnya
mapping_dict = merged_attendance.groupby('EmployeeNo').first().to_dict()
merged_attendance['EmployeeID'] = merged_attendance['EmployeeID'].fillna(merged_attendance['EmployeeNo'].map(mapping_dict['EmployeeID']))
merged_attendance['EmployeeOrgID'] = merged_attendance['EmployeeOrgID'].fillna(merged_attendance['EmployeeNo'].map(mapping_dict['EmployeeOrgID']))
merged_attendance['EmployeeName'] = merged_attendance['EmployeeName'].fillna(merged_attendance['EmployeeNo'].map(mapping_dict['EmployeeName']))

# join ke hrm employee untuk ambil kolom yang kosong yang absennya dari siknet doang
get_employee_org_id = pd.read_sql_query(""" SELECT employee_id as EmployeeIDNull, employee_org_id as EmployeeOrgIDNull,employee_ext_id as EmployeeNo, person_nm as EmployeeNameNull
                                            FROM xocp_hrm_employee WHERE status_cd = 'active'
                                        """, conn_ehr)

merged_attendance = merged_attendance.merge(get_employee_org_id, how='left',on='EmployeeNo')

# isi kolom dibawah ini yang kosong dari tabel hrm employee setelah di join 
merged_attendance['EmployeeID'].fillna(merged_attendance['EmployeeIDNull'],inplace=True)
merged_attendance['EmployeeOrgID'].fillna(merged_attendance['EmployeeOrgIDNull'],inplace=True)
merged_attendance['EmployeeName'].fillna(merged_attendance['EmployeeNameNull'],inplace=True)

# filter data yang NIP gaada di hrm_employee
merged_attendance=merged_attendance[~merged_attendance['EmployeeID'].isna()]

merged_attendance['EmployeeID'] = pd.to_numeric(merged_attendance['EmployeeID'], errors='coerce')
merged_attendance['EmployeeID'] = merged_attendance['EmployeeID'].apply(lambda x: int(round(float(x))))

merged_attendance['EmployeeOrgID'] = pd.to_numeric(merged_attendance['EmployeeOrgID'], errors='coerce')
merged_attendance['EmployeeOrgID'] = merged_attendance['EmployeeOrgID'].fillna(0)

# convert kolom jadi integer
merged_attendance['EmployeeID'] = merged_attendance['EmployeeID'].astype('int64')
merged_attendance['EmployeeOrgID'] = merged_attendance['EmployeeOrgID'].astype('int64') 

# remove columns yang ga kepake setelah join
columns_to_remove = ['EmployeeIDNull', 'EmployeeOrgIDNull', 'EmployeeNameNull']
merged_attendance = merged_attendance.drop(columns=columns_to_remove)

# remove data yang duplicate 
merged_attendance.drop_duplicates(inplace=True)

# bikin kolom baru working hours buat ngitung total jam kerja
merged_attendance['WorkingHours'] = (merged_attendance['AttendanceStop'] - merged_attendance['AttendanceStart'])
merged_attendance['WorkingHours'] = merged_attendance['WorkingHours'].apply(lambda x: '{:02}:{:02}:{:02}'.format(int(x.seconds // 3600), int((x.seconds % 3600) // 60), int(x.seconds % 60)))

# jika jam datang lebih besar, ubah workinghours jadi 00:00:00 untuk menghindari data anomali
mask = merged_attendance['AttendanceStart'] > merged_attendance['AttendanceStop']
merged_attendance.loc[mask, 'WorkingHours'] = pd.to_datetime('00:00:00').time()

# kalo yg ga absen dateng atau ga absen pulang, ubah working hoursnya jadi 00:00:00
mask = (merged_attendance['AttendanceStart'].dt.time == pd.to_datetime('00:00:00').time()) | (merged_attendance['AttendanceStop'].dt.time == pd.to_datetime('00:00:00').time())
merged_attendance.loc[mask, 'WorkingHours'] = pd.to_datetime('00:00:00').time()

# Bikin kondisi jika jam absen dateng dan pulang 00:00:00, maka IsAttend akan 0, selain itu jadi 1 (dianggap hadir)
condition = (merged_attendance["AttendanceStart"].dt.time == pd.to_datetime("00:00:00").time()) & (merged_attendance["AttendanceStop"].dt.time == pd.to_datetime("00:00:00").time())
merged_attendance['IsAttend'] = np.where(condition,0,1)

print('ini udah fix banget')
print(merged_attendance.iloc[:,[0,3,4,5,7,10]].sort_values(by=['EmployeeID','AttendanceDate']))
# Filter yang Dokter tidak mempunyai EmployeeNo (sudah tidak aktif)
merged_attendance = merged_attendance[merged_attendance['EmployeeNo'].notna()]

merged_attendance['IsSiknetStart'] = merged_attendance['IsSiknetStart'].astype('int64')
merged_attendance['IsSiknetStop'] = merged_attendance['IsSiknetStop'].astype('int64')

print(merged_attendance.dtypes)

if merged_attendance.empty:
    print('tidak ada data dari merged_attendance')
else:
    # jika dari merged_attendance cuma 1 row
    if len(merged_attendance) == 1:        
        # ambil primary key dari merged_attendance, ambil index ke 0
        employee_id = merged_attendance["EmployeeID"].values[0]
        attendance_date = merged_attendance["AttendanceDate"].values[0]

        # query buat narik data dari target lalu filter berdasarkan primary key
        query = f"SELECT EmployeeID, EmployeeOrgID,EmployeeNo,EmployeeName,AttendanceDate,AttendanceStart,IsSiknetStart,AttendanceStop,IsSiknetStop,WorkingHours,IsAttend FROM dwhrscm_talend.FactAttendanceEmployee where EmployeeID IN ({employee_id}) AND AttendanceDate IN ('{attendance_date}') order by EmployeeNo, AttendanceDate"
        target = pd.read_sql_query(query, conn_dwh_sqlserver)
    else :
         # ambil primary key dari merged_attendance, pake unique biar tidak duplicate
        employee_id = tuple(merged_attendance["EmployeeID"])
        attendance_date = tuple(merged_attendance["AttendanceDate"])

         # query buat narik data dari target lalu filter berdasarkan primary key
        query = f'SELECT EmployeeID, EmployeeOrgID,EmployeeNo,EmployeeName,AttendanceDate,AttendanceStart,IsSiknetStart,AttendanceStop,IsSiknetStop,WorkingHours,IsAttend FROM dwhrscm_talend.FactAttendanceEmployee where EmployeeID IN {employee_id} AND AttendanceDate IN {attendance_date} order by EmployeeNo, AttendanceDate'
        target = pd.read_sql_query(query, conn_dwh_sqlserver)

    target['AttendanceDate'] = pd.to_datetime(target.AttendanceDate, format='%Y-%m-%d')
    target['AttendanceDate'] = target['AttendanceDate'].dt.strftime('%Y-%m-%d')
    print('ini target')
    print(target.iloc[:,0:10])
    print(target.dtypes)

    merged_attendance['WorkingHours'] = pd.to_datetime(merged_attendance['WorkingHours'],format='%H:%M:%S')
    merged_attendance['WorkingHours'] = merged_attendance['WorkingHours'].dt.strftime('%H:%M:%S')

    target['WorkingHours'] = pd.to_datetime(target['WorkingHours'],format='%H:%M:%S')
    target['WorkingHours'] = target['WorkingHours'].dt.strftime('%H:%M:%S')
    # print(merged_attendance.iloc[:,0:3].apply(tuple,1).isin(target.iloc[:,0:3].apply(tuple,1)))
    # print(merged_attendance.iloc[:,3:5].apply(tuple,1).isin(target.iloc[:,3:5].apply(tuple,1)))
    # print(merged_attendance.iloc[:,5:7].apply(tuple,1).isin(target.iloc[:,5:7].apply(tuple,1)))
    # print(merged_attendance.iloc[:,8:9].apply(tuple,1).isin(target.iloc[:,8:9].apply(tuple,1)))
    # print(merged_attendance.iloc[:,9:11].apply(tuple,1).isin(target.iloc[:,9:11].apply(tuple,1)))
    # print(merged_attendance.iloc[:,9:10])
    # print(target.iloc[:,9:10])
    # print(merged_attendance.iloc[:,0:3].apply(tuple,1))
    # print(target.iloc[:,0:3].apply(tuple,1))
    # print(merged_attendance.iloc[:,2:4].apply(tuple,1))
    # print(target.iloc[:,2:4].apply(tuple,1))
    # print(merged_attendance.iloc[:,3:5].apply(tuple,1))
    # print(target.iloc[:,3:5].apply(tuple,1))
    # print(merged_attendance.iloc[:,4:6].apply(tuple,1))
    # print(target.iloc[:,4:6].apply(tuple,1))
    # print(merged_attendance.iloc[:,5:7].apply(tuple,1))
    # print(target.iloc[:,5:7].apply(tuple,1))
    # print(merged_attendance.iloc[:,7:10].apply(tuple,1))
    # print(target.iloc[:,7:10].apply(tuple,1))

    # ambil dataframe yang mengalami perubahan (termasuk data update dan data new)
    changes = merged_attendance[~merged_attendance.apply(tuple,1).isin(target.apply(tuple,1))]

    # ambil data yang update dari changes
    modified = changes[changes[['EmployeeID','AttendanceDate']].apply(tuple,1).isin(target[['EmployeeID','AttendanceDate']].apply(tuple,1))]
    total_row_upd = len(modified)
    text_upd = f'total row update : {total_row_upd}'
    print(text_upd)
    print(modified.iloc[:,[0,3,4,5,7,10]])

    # ambil data yang new dari changes
    inserted = changes[~changes[['EmployeeID','AttendanceDate']].apply(tuple,1).isin(target[['EmployeeID','AttendanceDate']].apply(tuple,1))]
    total_row_ins = len(inserted)
    text_ins = f'total row inserted : {total_row_ins}'
    print(text_ins)
    print(inserted.iloc[:,[0,3,4,5,7,10]])

    if modified.empty:
        if inserted.empty:
            print('tidak ada data yang baru dan update')
        else:
            try:   
                with conn_dwh_sqlserver.begin() as transaction:
                    inserted.to_sql('FactAttendanceEmployee', schema='dwhrscm_talend', con=conn_dwh_sqlserver, if_exists = 'append', index=False)
                    print('success insert all data without update')
            except Exception as e:
                print(e)
    else:
        # buat fungsi untuk update data ke tabel target
        def updated_to_sql(df, table_name, key_1,key_2):
            list_col = []
            table=table_name
            pk_1 = key_1
            pk_2 = key_2
            temp_table = f'{table}_temporary_table'
            for col in df.columns:
                if col == pk_1 or col == pk_2 :
                    continue
                list_col.append(f'r.{col} = t.{col}')
            df.to_sql(temp_table, schema= 'dwhrscm_talend',con=conn_dwh_sqlserver, if_exists='replace',index=False)
            update_stmt_1 = f'UPDATE r '
            update_stmt_2 = f'SET '
            update_stmt_3 = ", ".join(list_col)
            update_stmt_8 = f' , r.UpdatedDateDWH = CONVERT(DATETIME2(0), GETDATE(),120)'
            update_stmt_4 = f' FROM dwhrscm_talend.{table} r '
            update_stmt_5 = f'INNER JOIN (SELECT * FROM dwhrscm_talend.{temp_table}) as t ON r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} '  
            update_stmt_6 = f' WHERE r.{pk_1} = t.{pk_1} AND r.{pk_2} = t.{pk_2} '
            update_stmt_7 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_8 + update_stmt_4 + update_stmt_5 + update_stmt_6 +";"
            delete_stmt_1 = f'DROP TABLE dwhrscm_talend.{temp_table}'
            print(update_stmt_7)
            conn_dwh_sqlserver.execute(update_stmt_7)
            conn_dwh_sqlserver.execute(delete_stmt_1)

        try:
            with conn_dwh_sqlserver.begin() as transaction:
                # update data
                updated_to_sql(modified, 'FactAttendanceEmployee', 'EmployeeID', 'AttendanceDate')

                #insert data baru
                inserted.to_sql('FactAttendanceEmployee', schema='dwhrscm_talend',con=conn_dwh_sqlserver, if_exists ='append',index=False)
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
db_connection.close_connection(conn_attendance)
db_connection.close_connection(conn_siknet)

