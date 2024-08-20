import json
import pandas as pd
import  requests
from requests.auth import HTTPBasicAuth
from sqlalchemy import create_engine
import smtplib,ssl
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import formatdate
from email import encoders
import datetime as dt
from datetime import datetime, timedelta
from openpyxl.styles import Alignment
import sys
from babel.numbers import format_currency


def send_mail(send_from, send_to, subject, text, server, port, username='', password='', filename=None):
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = ', '.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject
    msg.attach(MIMEText(text))

    if filename is not None:
        part = MIMEBase('application', "octet-stream")
        part.set_payload(open(filename, "rb").read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename={filename}')
        msg.attach(part)

    smtp = smtplib.SMTP_SSL(server, port)
    smtp.login(username, password)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.quit()

engine = create_engine('postgresql://orpctbsqvqtnrx:530428203217ce11da9eb9586a5513d0c7fe08555c116c103fd43fb78a81c944@ec2-34-202-53-101.compute-1.amazonaws.com:5432/d46bn1u52baq92',\
                           echo = False)

vip_base = pd.read_sql_query('''with deposits as ( \
select "ClientId", count(DISTINCT "Id") as dpst_count, sum("ConvertedAmount") as dpst_amt, \
max(date("CreationTime")) as last_dpst_date  from  customer_transactions_betfoxx \
where "Status" in ('Approved', 'ApprovedManually') \
and "Type" = 2 \
group by 1), \
 \
deposits_rlp as ( \
SELECT \
    "ClientId", \
    date("CreationTime") AS dpst_date, \
    COUNT(DISTINCT "Id") AS dpst_count, \
    SUM("ConvertedAmount") AS dpst_amt, \
    SUM(SUM("ConvertedAmount")) OVER (PARTITION BY "ClientId" ORDER BY date("CreationTime")) AS cumulative_dpst_amt, \
    SUM(COUNT(DISTINCT "Id")) OVER (PARTITION BY "ClientId" ORDER BY date("CreationTime")) AS cumulative_dpst_count \
FROM \
    customer_transactions_betfoxx \
WHERE \
    "Status" IN ('Approved', 'ApprovedManually') \
    AND "Type" = 2 \
GROUP BY \
    "ClientId", \
    dpst_date \
ORDER BY \
    "ClientId", \
    dpst_date), \
\
vip as (select "ClientId",  min("dpst_date") as vip_date from deposits_rlp \
    where cumulative_dpst_amt >= 200 or cumulative_dpst_count >= 5 \
    group  by  1), \
 \
wthdrls as ( \
select "ClientId", sum("ConvertedAmount") as wtdrl_amt  from  customer_transactions_betfoxx \
where "Status" in ('Approved', 'ApprovedManually') \
and "Type" = 3 \
group by 1) \
 \
select a.*, b."FirstName", b."LastName",b."Email",b."BirthDate",b."Address",b."CountryName", b."AffiliateId","LanguageId", date("LastSessionDate") as last_login_date, wtdrl_amt,"RealBalance","BonusBalance", vip_date, \
 CURRENT_DATE - DATE(b."LastSessionDate") AS days_since_last_login, \
 CURRENT_DATE - Last_dpst_date AS days_since_last_dpst \
from deposits as a \
left join customers_betfoxx as b \
on a."ClientId" = b."Id" \
left join wthdrls as c \
on a."ClientId" = c."ClientId" \
left join vip as d \
on a."ClientId" = d."ClientId" \
where vip_date is not null ''', con=engine)

vip_base.columns

vip_base = vip_base.fillna(0)

vip_base["dpst_amt"] = vip_base["dpst_amt"].apply(lambda x: format_currency(x, currency="EUR", locale="nl_NL"))
vip_base["RealBalance"] = vip_base["RealBalance"].apply(lambda x: format_currency(x, currency="EUR", locale="nl_NL"))
vip_base["BonusBalance"] = vip_base["BonusBalance"].apply(lambda x: format_currency(x, currency="EUR", locale="nl_NL"))

date = dt.datetime.today()-  timedelta(1)
date_1 = date.strftime("%m-%d-%Y")
filename = f'Betfoxx_VIP_Cucstomers_{date_1}.xlsx'
sub = f'Betfoxx_VIP_Customers_{date_1}'

with pd.ExcelWriter(filename, engine='openpyxl') as writer:
    vip_base.to_excel(writer, sheet_name="VIP_Summary", index=False)

subject = sub
body = f"Hi,\n\n Attached contains the VIP Customers summary for the {date_1} for Betfoxx \n\nThanks,\nSaketh"
sender = "sakethg250@gmail.com"
recipients = ["lina@crystalwg.com","sandra@crystalwg.com","camila@crystalwg.com","isaac@crystalwg.com","sebastian@crystalwg.com","sakethg250@gmail.com","saketh@crystalwg.com"]
password = "xjyb jsdl buri ylqr"
send_mail(sender, recipients, subject, body, "smtp.gmail.com", 465, sender, password, filename)