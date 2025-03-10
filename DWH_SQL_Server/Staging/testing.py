import sys
sys.path.insert(1,'C://TestPython//connection')
import db_connection
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

log = "Log Script/script.log"
# Setup logging to capture all levels
logging.basicConfig(
    filename= log,
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
    """Main script logic."""
    try:
        logging.info("Script started.")
        
        conn_staging_sqlserver = db_connection.create_connection(db_connection.staging_sqlserver)
        logging.debug("Running script logic...")
        # Simulate an error for testing
        logging.warning("Simulated warning: Database connection might be slow.")
        
    
    except Exception as e:
        # Log the error
        error_message = f"Script failed with error: {str(e)}"
        logging.error(error_message)
        
        # Send email notification
        send_email(subject="Script Failure Notification", body=error_message)
    finally:
        logging.info("Script finished.")

if __name__ == "__main__":
    main()
