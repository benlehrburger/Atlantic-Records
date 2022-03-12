import email, smtplib, ssl
from email import encoders
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# SEND CSV FILES IN AN EMAIL

YOUR_EMAIL = ''
YOUR_EMAIL_PASSWORD = ''
RECIPIENT_EMAIL = ''


# Send email with CSV attachments
def send_email():
    stat = '10%'
    port = 465
    smtp_server = 'smtp.gmail.com'
    sender_email = YOUR_EMAIL
    password = YOUR_EMAIL_PASSWORD
    receiver_email = RECIPIENT_EMAIL
    message = MIMEMultipart("alternative")
    message["Subject"] = "multipart test"
    message["From"] = sender_email
    message["To"] = receiver_email

    files = ["broad_results.csv", "hottest_sounds.csv"]

    subject = "Email with attachment"
    body = "Hi Atlantic team,\n\nToday's analysis of TikTok sound performance is attached below."
    message.attach(MIMEText(body, "plain"))

    for file in files:
        insert_attachment(message, file)

    text = message.as_string()

    context = ssl.create_default_context()

    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, text)


# Helper method for adding an attachment to an email
def insert_attachment(message, filename):
    with open(filename, "rb") as attachment:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())

    encoders.encode_base64(part)

    part.add_header(
        "Content-Disposition",
        f"attachment; filename= {filename}",
    )

    message.attach(part)