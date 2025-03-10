import os
from sqlalchemy import create_engine as ce
from dotenv import load_dotenv

# load credential from env and pass to environment
load_dotenv()

# DB MPI
MPI_DB_HOST = os.getenv("MPI_DB_HOST")
MPI_DB_USER = os.getenv("MPI_DB_USER")
MPI_DB_PASS = os.getenv("MPI_DB_PASS")
MPI_DB_NAME = os.getenv("MPI_DB_NAME")

# DB Replika EHR
REP_EHR_DB_HOST = os.getenv("REP_EHR_DB_HOST") 
REP_EHR_DB_USER = os.getenv("REP_EHR_DB_USER")
REP_EHR_DB_PASS = os.getenv("REP_EHR_DB_PASS")
REP_EHR_DB_NAME = os.getenv("REP_EHR_DB_NAME")

# DB EHR LIVE 
EHR_LIVE_DB_HOST = os.getenv("EHR_LIVE_DB_HOST")
EHR_LIVE_DB_USER = os.getenv("EHR_LIVE_DB_USER")
EHR_LIVE_DB_PASS = os.getenv("EHR_LIVE_DB_PASS")
EHR_LIVE_DB_NAME  = os.getenv("EHR_LIVE_DB_NAME")

# DB HIS LIVE
HIS_LIVE_DB_HOST = os.getenv("HIS_LIVE_DB_HOST")
HIS_LIVE_DB_USER = os.getenv("HIS_LIVE_DB_USER")
HIS_LIVE_DB_PASS = os.getenv("HIS_LIVE_DB_PASS")
HIS_LIVE_DB_NAME  = os.getenv("HIS_LIVE_DB_NAME")

# DB Staging SQL Server
STAGING_DB_HOST = os.getenv("STAGING_DB_HOST")
STAGING_DB_USER = os.getenv("STAGING_DB_USER")
STAGING_DB_PASS = os.getenv("STAGING_DB_PASS") 
STAGING_DB_NAME = os.getenv("STAGING_DB_NAME")

# DB DWH SQL Server
DWH_DB_HOST = os.getenv("DWH_DB_HOST")
DWH_DB_USER = os.getenv("DWH_DB_USER")
DWH_DB_PASS = os.getenv("DWH_DB_PASS") 
DWH_DB_NAME = os.getenv("DWH_DB_NAME")

# DB Attendance 
ATTENDANCE_DB_HOST = os.getenv("ATTENDANCE_DB_HOST")
ATTENDANCE_DB_USER = os.getenv("ATTENDANCE_DB_USER")
ATTENDANCE_DB_PASS = os.getenv("ATTENDANCE_DB_PASS")
ATTENDANCE_DB_NAME  = os.getenv("ATTENDANCE_DB_NAME")

# DB Siknet
SIKNET_DB_HOST = os.getenv("SIKNET_DB_HOST")
SIKNET_DB_USER = os.getenv("SIKNET_DB_USER")
SIKNET_DB_PASS = os.getenv("SIKNET_DB_PASS")
SIKNET_DB_NAME  = os.getenv("SIKNET_DB_NAME")

# Email Notifications 
email_sender = os.getenv("EMAIL_SENDER")
email_receiver = os.getenv("EMAIL_RECEIVER")
email_password = os.getenv("EMAIL_PASSWORD") 

# Create SQLAlchemy engine objects for each database
mpi = ce(f"mysql://{MPI_DB_USER}:{MPI_DB_PASS}@{MPI_DB_HOST}/{MPI_DB_NAME}")
# replika_ehr = ce('mysql://hanif-ppi:hanif2022@172.16.19.11/ehr')
replika_ehr = ce(f"mysql://{REP_EHR_DB_USER}:{REP_EHR_DB_PASS}@{REP_EHR_DB_HOST}/{REP_EHR_DB_NAME}")
replika_his =  ce('mysql://hanif-ppi:hanif2022@172.16.19.21/his')
dwh_mysql = ce('mysql://hanif-ppi:hanif2022@172.16.5.33/dwhrscm_talend')
dwh_sqlserver = ce(f"mssql+pyodbc://{DWH_DB_USER}:{DWH_DB_PASS}@{DWH_DB_HOST}/{DWH_DB_NAME}?driver=SQL+Server+Native+Client+11.0", fast_executemany=True)
staging_sqlserver = ce(f"mssql+pyodbc://{STAGING_DB_USER}:{STAGING_DB_PASS}@{STAGING_DB_HOST}/{STAGING_DB_NAME}?driver=SQL+Server+Native+Client+11.0", fast_executemany=True)
bios_sqlserver = ce('mssql+pyodbc://andhi-ppi:Andhi2022!@172.16.19.36/BIOS?driver=SQL+Server+Native+Client+11.0', fast_executemany=True)
ehr_live = ce(f"mysql://{EHR_LIVE_DB_USER}:{EHR_LIVE_DB_PASS}@{EHR_LIVE_DB_HOST}/{EHR_LIVE_DB_NAME}")
his_live = ce(f"mysql://{HIS_LIVE_DB_USER}:{HIS_LIVE_DB_PASS}@{HIS_LIVE_DB_HOST}/{HIS_LIVE_DB_NAME}")
attendance = ce(f"mysql://{ATTENDANCE_DB_USER}:{ATTENDANCE_DB_PASS}@{ATTENDANCE_DB_HOST}/{ATTENDANCE_DB_NAME}")
siknet = ce(f"mysql://{SIKNET_DB_USER}:{SIKNET_DB_PASS}@{SIKNET_DB_HOST}/{SIKNET_DB_NAME}")
transmedic = ce('postgresql+psycopg2://hanif-ppi:hanif2022@172.16.7.120:5432/transmedic-lat')

# Function to create and return a session based on the given engine
def create_connection(engine):
    return engine.connect()

# Function to close the session
def close_connection(connection):
    connection.close()
