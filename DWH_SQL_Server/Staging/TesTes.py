import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import datetime as dt
date = dt.datetime.today()
import time
from datetime import timedelta
import pandas as pd
import pyodbc
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart



# bikin log ke file
sys.stdout = open("C:/TestPython/DWH_SQL_Server/Staging/logs/LogDimensionPatientMPINew.txt","w")
t0 = time.time()


# log path file error
log_file_path = "C:/TestPython/DWH_SQL_Server/Staging/Log/TesTes.log"


# Setup logging to capture all levels
logging.basicConfig(
    filename=log_file_path,
    level=logging.DEBUG,  # Set the lowest level to capture everything
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def send_email(subject, body):
    """Send an email notification."""
    try:
        logging.info("Preparing to send email notification.")
        
        # Email configuration
        sender_email = "lawkiddd2806@gmail.com"  # Replace with your email
        receiver_email = "lawkiddd2806@gmail.com"  # Replace with receiver's email
        password = "xoknfjvawuxitjvz"  # Replace with your app-specific password

        # Create the email
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = receiver_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))
        
        # Connect to the mail server and send email
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            logging.debug("Connecting to SMTP server.")
            server.starttls()
            server.login(sender_email, password)
            server.send_message(msg)
        logging.info("Email sent successfully.")
    except Exception as e:
        logging.error("Failed to send email notification: %s", str(e))

def main():
    try:
        # bikin koneksi ke db
       
        conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)
        conn_mpi = db_connection.create_connection(db_connection.mpi)

    
    except Exception as e:
        # Log The Error
        error_message = (
            f"Dear Hanif,\n\n"
            f"There is an error in your script to DimPatientMPI, here's the error :\n\n"
            f"{str(e)}\n\n"
            f"Please investigate the issue.\n\n"
            f"Best regards,\nYour Monitoring Script"
        )
        logging.error=error_message
        
        # Send email notification
        send_email(subject="Alert Error For DimensionPatientMPI", body=error_message)

    finally:
        logging.info("Script finished.")
        db_connection.close_connection(conn_mpi)
        db_connection.close_connection(conn_staging_sqlserver)

if __name__ == "__main__":
    main()

