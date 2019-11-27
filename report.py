from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
from irods.session import iRODSSession
from irods.models import Collection, DataObject
from config import YODATEST, AIMMS, SURF, MAIL_TO, MAIL_FROM, SMTP_HOST


def send_mail(subject, text):
    SUBJECT = subject
    message = MIMEMultipart()
    message.attach(MIMEText(text))
    message['From'] = MAIL_FROM
    message['To'] = MAIL_TO
    message['Subject'] = SUBJECT
    try:
        s = smtplib.SMTP(SMTP_HOST)
        s.send_message(message)
        s.quit()
    except Exception as e:
        pass


def convert_bytes(num):
    """
    this function will convert bytes to MB.... GB... etc
    """
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0


def query(connection):
    with iRODSSession(host=connection['host'], port=connection['port'], user=connection['user'],
                      password=connection['password'], zone=connection['zone'], ssl_verify_server="none",
                      home='/tempZone/home', authentication_scheme='pam') as session:
        query = session.query(Collection.name, DataObject.id, DataObject.size)
        rep = {}
        total_size = 0
        for result in query:
            a = result[Collection.name].split('/')
            top = '{}/{}/{}/{}'.format(a[0], a[1], a[2], a[3])
            size = result[DataObject.size]
            total_size = total_size + size
            if top not in rep:
                rep[top] = {}
                rep[top]['size'] = 0
                rep[top]['count'] = 0
            rep[top]['size'] = rep[top]['size'] + size
            rep[top]['count'] = rep[top]['count'] + 1
    return rep, total_size


conn = SURF
rep, used = query(conn)
percentage = (used / conn['size']) * 100
available = conn['size'] - used

text = 'used: {}\n'.format(convert_bytes(used))
text = text + 'size: {}\n'.format(convert_bytes(conn['size']))
text = text + 'available: {}\n'.format(convert_bytes(available))
text = text + 'percentage: {}\n\n'.format(round(percentage, 2))
for coll in sorted(rep):
    text = text + '{}\t{}\t{}\n'.format(coll, rep[coll]['count'], convert_bytes(rep[coll]['size']))
print(text)
warning=75
if percentage > warning:
    subject = 'WARNING: storage on {} over {} full!!'.format(conn['host'], warning)
else:
    subject = 'INFO: storage usage on {}'.format(conn['host'])
send_mail(subject, text)
