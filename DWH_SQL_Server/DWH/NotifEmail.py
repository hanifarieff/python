import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

sender_email = "sender+email@mail.com"
sender_password = "wabsqstlctgygoad"

server = smtplib.SMTP_SSL('smtp.googlemail.com', 465)
server.login(sender_email, sender_password)

receiver_emails = ["receiver+email1@mail.com","receiver+email2@mail.com"]
for receiver_email in receiver_emails:
    msg = MIMEMultipart('alternative')
    msg['Subject'] = "Test mail"
    msg['From'] = sender_email
    msg['To'] = receiver_email
    html = """
        <h1>Hello world</h1>
    """
    msg.attach(MIMEText(html, 'html'))
    server.sendmail(sender_email, receiver_email, msg.as_string())

server.quit()