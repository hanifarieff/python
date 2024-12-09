from sqlalchemy import create_engine as ce

# Create SQLAlchemy engine objects for each database
mpi = ce("mysql://hanif-ppi:hanif2022@172.16.6.10/mpi")
# replika_ehr = ce('mysql://hanif-ppi:hanif2022@172.16.19.11/ehr')
replika_ehr = ce('mysql://hanif-ppi:hanif2022@192.168.119.9/ehr')
replika_his =  ce('mysql://hanif-ppi:hanif2022@172.16.19.21/his')
dwh_mysql = ce('mysql://hanif-ppi:hanif2022@172.16.5.33/dwhrscm_talend')
dwh_sqlserver = ce('mssql+pyodbc://andhi-ppi:Andhi2022!@172.16.19.36/DWH_RSCM?driver=SQL+Server+Native+Client+11.0', fast_executemany=True)
staging_sqlserver = ce('mssql+pyodbc://andhi-ppi:Andhi2022!@172.16.19.36/StagingRSCM?driver=SQL+Server+Native+Client+11.0', fast_executemany=True)
bios_sqlserver = ce('mssql+pyodbc://andhi-ppi:Andhi2022!@172.16.19.36/BIOS?driver=SQL+Server+Native+Client+11.0', fast_executemany=True)
ehr_live = ce("mysql://hanif-ppi:hanif2022@192.168.119.8/ehr")
his_live = ce('mysql://hanif-ppi:hanif2022@192.168.119.2/his')
attendance = ce('mysql://hanif-ppi:hanif2022@192.168.119.2/db_attendance')
siknet = ce('mysql://hanif-ppi:hanif2022@172.16.19.32/idpadmin')
transmedic = ce('postgresql+psycopg2://hanif-ppi:hanif2022@172.16.7.120:5432/transmedic-lat')

# Function to create and return a session based on the given engine
def create_connection(engine):
    print("success connected to ", engine)
    return engine.connect()

# Function to close the session
def close_connection(connection):
    connection.close()
