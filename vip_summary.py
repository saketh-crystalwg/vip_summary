import pandas as pd
import datetime as dt
from datetime import datetime, timedelta
from babel.numbers import format_currency
import numpy as np

import mysql.connector
from mysql.connector import Error

from sqlalchemy import create_engine

try:
    connection = mysql.connector.connect(host='206.189.96.57',
                                         database='platform',
                                         user='PlatBI',
                                         password='BIAIPass!2019204PurumPum')
    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        cursor = connection.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)

except Error as e:
    print("Error while connecting to MySQL", e)

VIP_Summary = pd.read_sql_query("with base as ( \
select a.customer_fk, c.name as merchant_name,referral_info, d.country_desc, \
DATEDIFF(SYSDATE(),date(b.last_activity_time)) as days_since_last_login, \
e.lang_desc, email, login, DATEDIFF(SYSDATE(),date(a.full_reg_complete)) as days_since_register \
from platform.customer_attributes as a \
left join platform.customers as b \
on a.customer_fk = b.id \
left join platform.merchants as c \
on b.merchant_fk = c.id \
left join platform.countries as d \
on b.country_fk = d.id \
left join platform.languages as e \
on b.language_fk = e.id \
) \
, \
txn_base as ( \
select a.*, b.rate_to_eur from platform.customer_transactions as a  \
left join (select id, rate_to_eur from platform.currencies c ) as b \
on a.currency_fk = b.id \
where status in ('APPROVED', 'SUCCESSFUL') \
), \
dpst_rlp as ( \
select customer_fk, date(c_date) as txn_date, count(id)  as dpst_cnt, \
DATEDIFF(SYSDATE(),date(c_date)) as date_diff_dpst, \
sum(amount/rate_to_eur) as  deposit_amount_eur from txn_base \
where trx_type = 'DEPOSIT' \
group by 1,2), \
wtdrl_rlp as ( \
select customer_fk, date(c_date) as wtdrl_date, \
DATEDIFF(SYSDATE(),date(c_date)) as date_diff_wtdrl, \
count(id)  as withdrawl_cnt,sum(amount/rate_to_eur) as  wtdrl_amount_eur from txn_base \
where trx_type = 'WITHDRAWAL' \
group by 1,2), \
net_dpst as ( \
select customer_fk, sum(case when trx_type = 'DEPOSIT' then amount/rate_to_eur else 0 end) as total_deposits, \
sum(case when trx_type = 'WITHDRAWAL' then amount/rate_to_eur else 0 end) as total_withdrawals \
from txn_base \
group by 1 \
), \
bet_day as ( \
select customer_fk, max(summary_day) as last_bet_date from platform.games_per_customer_summaries gpcs \
where total_stakes_base_currency > 0 \
group by 1), \
\
balance_base as ( \
select customer_fk, max(summary_day) as max_date from platform.customer_money_summaries cms \
group by 1 \
), \
\
balance as ( \
select a.customer_fk, balance_base_currency from platform.customer_money_summaries as  a \
inner join balance_base as b \
on a.customer_fk = b.customer_fk \
and a.summary_day = b.max_date \
), \
\
revenues as ( \
select customer_fk, summary_day, \
DATEDIFF(SYSDATE(),summary_day) as date_diff_rev, \
sum(NGR_base_currency) as NGR, sum(total_stakes_base_currency) as bets, \
sum(finished_games_real_money) as games, \
sum(GGR_base_currency) as GGR, \
sum(total_returns_base_currency) as  returns_1 \
from platform.games_per_customer_summaries \
group by 1,2 ), \
\
average_games as (select customer_fk, (sum(finished_games_real_money)/ count(DISTINCT summary_day))  as avg_games_cnt from platform.games_per_customer_summaries gpcs \
group by 1 \
order by customer_fk desc \
), \
\
dpst_f as ( \
select customer_fk, sum(case when date_diff_dpst <= 7 then deposit_amount_eur else 0  end ) as deposit_7_days, \
sum(case when date_diff_dpst <= 14 then deposit_amount_eur else 0  end ) as deposit_14_days, \
sum(case when date_diff_dpst <= 21 then deposit_amount_eur else 0  end ) as deposit_21_days, \
sum(case when date_diff_dpst <= 32 then deposit_amount_eur else 0  end ) as deposit_32_days, \
sum(case when date_diff_dpst <= 60 then deposit_amount_eur else 0  end ) as deposit_60_days, \
sum(case when date_diff_dpst <= 90 then deposit_amount_eur else 0  end ) as deposit_90_days, \
sum(deposit_amount_eur) as deposit_lifetime, \
sum(dpst_cnt) as total_deposits_count, \
min(date_diff_dpst) as days_since_last_deposit ,\
max(date_diff_dpst) as days_since_first_deposit \
from dpst_rlp \
group by 1), \
\
wtdrl_f as ( \
select customer_fk, sum(case when date_diff_wtdrl <= 7 then wtdrl_amount_eur else 0  end ) as Withdrawl_7_Days, \
sum(case when date_diff_wtdrl <= 14 then wtdrl_amount_eur else 0  end ) as Withdrawl_14_Days, \
sum(case when date_diff_wtdrl <= 21 then wtdrl_amount_eur else 0  end ) as Withdrawl_21_Days, \
sum(case when date_diff_wtdrl <= 32 then wtdrl_amount_eur else 0  end ) as Withdrawl_32_Days, \
sum(case when date_diff_wtdrl <= 60 then wtdrl_amount_eur else 0  end ) as Withdrawl_60_Days, \
sum(case when date_diff_wtdrl <= 90 then wtdrl_amount_eur else 0  end ) as Withdrawl_90_Days, \
sum(wtdrl_amount_eur) as Withdrawl_lifetime, \
sum(withdrawl_cnt) as withdrawl_count \
from wtdrl_rlp \
group by 1), \
\
revenues_f as ( \
select customer_fk, sum(case when date_diff_rev <= 7 then NGR else 0  end ) as NGR_7_Days, \
sum(case when date_diff_rev <= 14 then NGR else 0  end ) as NGR_14_Days, \
sum(case when date_diff_rev <= 21 then NGR else 0  end ) as NGR_21_Days, \
sum(case when date_diff_rev <= 32 then NGR else 0  end ) as NGR_32_Days, \
sum(case when date_diff_rev <= 60 then NGR else 0  end ) as NGR_60_Days, \
sum(case when date_diff_rev <= 90 then NGR else 0  end ) as NGR_90_Days, \
sum(NGR) as NGR_lifetime, \
sum(GGR) as GGR_lifetime, \
(sum(case when date_diff_rev <= 7 then bets else 0  end ) / sum(case when date_diff_rev <= 7 then games else 0  end ))  as Avg_Bet_7_Days, \
(sum(case when date_diff_rev <= 14 then bets else 0  end ) / sum(case when date_diff_rev <= 14 then games else 0  end )) as Avg_Bet_14_Days, \
(sum(case when date_diff_rev <= 21 then bets else 0  end ) / sum(case when date_diff_rev <= 21 then games else 0  end )) as Avg_Bet_21_Days, \
(sum(case when date_diff_rev <= 32 then bets else 0  end ) / sum(case when date_diff_rev <= 32 then games else 0  end )) as Avg_Bet_32_Days, \
(sum(case when date_diff_rev <= 60 then bets else 0  end ) / sum(case when date_diff_rev <= 60 then games else 0  end )) as Avg_Bet_60_Days, \
(sum(case when date_diff_rev <= 90 then bets else 0  end ) / sum(case when date_diff_rev <= 90 then games else 0  end )) as Avg_Bet_90_Days, \
sum(returns_1)/sum(bets) as Payout_Percent \
from revenues \
group by 1), \
\
1k_base as ( \
select customer_fk, (amount/rate_to_eur) as amount_euro, \
date(c_date) as txn_date  from platform.customer_transactions as a \
left join (select distinct id, rate_to_eur from platform.currencies where is_valid = 1) as b \
on a.currency_fk = b.id \
where trx_type = 'DEPOSIT' \
and status in ('APPROVED','SUCCESSFUL')), \
\
1k_base_1 as ( \
select customer_fk, txn_date, sum(amount_euro) as deposits from 1k_base \
group by 1,2), \
\
1k_base_2 as ( \
select customer_fk, txn_date,deposits, \
sum(deposits) over ( PARTITION by customer_fk order by txn_date asc ) as total_dpst from 1k_base_1), \
 \
1k_base_3 as ( \
select *, ROW_NUMBER()over(PARTITION by customer_fk order by txn_date asc) as 750_date  from 1k_base_2 \
where total_dpst >= 750), \
\
1k_base_4 as ( \
select a.customer_fk, txn_date as date_of_reaching_750 from 1k_base_3 as a \
left join platform.customers as b \
on a.customer_fk = b.id \
where 750_date = 1), \
\
bonus_base as ( \
select a.*, b.currency_fk, c.rate_to_eur, (a.winning_amount /c.rate_to_eur) as win_amount_euro, \
d.promo_code, d.description, e.name as merchant_name, f.referral_info, g.country_desc as country_name, \
date_of_reaching_750,date(a.c_date) as bonus_date \
from platform.customer_bonuses as a \
left join platform.customers as b \
on a.customer_fk  = b.id \
left join platform.currencies as c \
on b.currency_fk = c.id \
left join platform.bonuses as d \
on a.bonus_fk = d.id \
left join platform.merchants as e \
on a.merchant_fk = e.id \
left join platform.customer_attributes as f \
on a.customer_fk = f.customer_fk \
left join platform.countries as g \
on b.country_fk = g.id \
left join 1k_base_4 as h \
on a.customer_fk = h.customer_fk \
where a.status  = 'WIN'), \
\
bonus_base_1 as ( \
SELECT customer_fk, \
sum(win_amount_euro) as mkt_expense, \
sum(case when win_amount_euro > 0  then 1 else  0  end) as bonus_count, \
sum(case when win_amount_euro > 0 and (bonus_date >=  date_of_reaching_750 ) then 1 else  0  end) as vip_bonus_count \
from bonus_base as a \
group by 1 \
having mkt_expense > 0) \
\
select a.customer_fk as Customer_ID, \
a.merchant_name as Brand, \
a.referral_info as Affiliate_ID, \
a.country_desc as  Country, \
a.lang_desc as Language, \
Days_since_last_login, \
DATEDIFF(SYSDATE(),date(f.last_bet_date)) as Days_since_last_bet, \
Days_since_last_deposit, \
Deposit_7_days, Deposit_14_days,Deposit_21_days, Deposit_32_days, Deposit_60_days, Deposit_90_days, Total_Deposits_Count, Deposit_Lifetime, \
Withdrawl_7_Days, Withdrawl_14_Days,Withdrawl_21_Days, Withdrawl_32_Days, Withdrawl_60_Days, Withdrawl_90_Days, Withdrawl_Lifetime, \
Avg_Bet_7_Days, Avg_Bet_14_Days,Avg_Bet_21_Days, Avg_Bet_32_Days, Avg_Bet_60_Days, Avg_Bet_90_Days, \
NGR_7_Days, NGR_14_Days,NGR_21_Days, NGR_32_Days, NGR_60_Days, NGR_90_Days, NGR_lifetime, GGR_Lifetime, \
(NGR_7_Days/deposit_7_days) as NGR_Deposits_7_Days, (NGR_14_Days/deposit_14_days) as NGR_Deposits_14_Days, \
(NGR_21_Days/deposit_21_days) as NGR_Deposits_21_Days, \
(NGR_32_Days/deposit_32_days) as NGR_Deposits_32_Days, (NGR_60_Days/deposit_60_days) as NGR_Deposits_60_Days, \
(NGR_90_Days/deposit_90_days) as NGR_Deposits_90_Days, balance_base_currency as Player_Balance, \
(Withdrawl_Lifetime / Deposit_Lifetime ) as Deposit_Payout_Percent, Payout_Percent, \
(Deposit_Lifetime - Withdrawl_Lifetime )  as Net_deposits, mkt_expense as Bonus_Used,date_of_reaching_750, \
case when email like '%blocked%' then 1 else 0 end as is_blocked, login as username , withdrawl_count, days_since_first_deposit, \
days_since_register, bonus_count, vip_bonus_count, email \
from base as a \
left join revenues_f as b \
on a.customer_fk = b.customer_fk \
left join wtdrl_f as c \
on a.customer_fk = c.customer_fk \
left join dpst_f as d \
on a.customer_fk = d.customer_fk \
left join balance as e \
on a.customer_fk = e.customer_fk \
left join bet_day as f \
on a.customer_fk = f.customer_fk \
left join bonus_base_1 as g \
on a.customer_fk = g.customer_fk \
left join 1k_base_4 as i \
on a.customer_fk = i.customer_fk", con=connection)

VIP_Summary[["Deposit_7_days","Deposit_14_days","Deposit_21_days","Deposit_32_days",\
            "Deposit_60_days","Deposit_90_days","Total_Deposits_Count",\
            "Deposit_Lifetime","Withdrawl_7_Days",\
            "Withdrawl_14_Days","Withdrawl_21_Days","Withdrawl_32_Days",\
            "Withdrawl_60_Days","Withdrawl_90_Days","Withdrawl_Lifetime",\
            'Avg_Bet_7_Days','Avg_Bet_14_Days','Avg_Bet_21_Days', 'Avg_Bet_32_Days', 'Avg_Bet_60_Days',\
             'Avg_Bet_90_Days', 'NGR_7_Days', 'NGR_14_Days','NGR_21_Days', 'NGR_32_Days',\
             'NGR_60_Days', 'NGR_90_Days', 'NGR_lifetime', 'GGR_Lifetime',\
             'NGR_Deposits_7_Days', 'NGR_Deposits_14_Days','NGR_Deposits_21_Days', 'NGR_Deposits_32_Days',\
             'NGR_Deposits_60_Days', 'NGR_Deposits_90_Days', 'Player_Balance',\
             'Deposit_Payout_Percent','Payout_Percent', 'Net_deposits', 'Bonus_Used']]\
= VIP_Summary[["Deposit_7_days","Deposit_14_days","Deposit_21_days","Deposit_32_days",\
            "Deposit_60_days","Deposit_90_days","Total_Deposits_Count",\
            "Deposit_Lifetime","Withdrawl_7_Days",\
            "Withdrawl_14_Days","Withdrawl_21_Days","Withdrawl_32_Days",\
            "Withdrawl_60_Days","Withdrawl_90_Days","Withdrawl_Lifetime",\
            'Avg_Bet_7_Days','Avg_Bet_14_Days','Avg_Bet_21_Days', 'Avg_Bet_32_Days', 'Avg_Bet_60_Days',\
             'Avg_Bet_90_Days', 'NGR_7_Days', 'NGR_14_Days','NGR_21_Days', 'NGR_32_Days',\
             'NGR_60_Days', 'NGR_90_Days', 'NGR_lifetime', 'GGR_Lifetime',\
             'NGR_Deposits_7_Days', 'NGR_Deposits_14_Days','NGR_Deposits_21_Days', 'NGR_Deposits_32_Days',\
             'NGR_Deposits_60_Days', 'NGR_Deposits_90_Days', 'Player_Balance',\
             'Deposit_Payout_Percent','Payout_Percent', 'Net_deposits', 'Bonus_Used']].apply(lambda x:round(x,2))

VIP_Summary['VIP_Segment'] = ['VIP Customer' if x >= 1000 \
                              else 'Pre VIP Customer' if x >= 750  and x < 1000 \
                             else 'Potential VIP Customer' if (x >= 250 and x < 750) or y >= 10 \
                              else 'Non VIP Customers' for x,y in zip(VIP_Summary['Deposit_Lifetime'],VIP_Summary['Total_Deposits_Count'])]

VIP_Summary_1 = VIP_Summary[VIP_Summary['VIP_Segment'] != 'Non VIP Customers']


engine = create_engine('postgresql://orpctbsqvqtnrx:530428203217ce11da9eb9586a5513d0c7fe08555c116c103fd43fb78a81c944@ec2-34-202-53-101.compute-1.amazonaws.com:5432/d46bn1u52baq92',\
                           echo = False)

help_desk_info = pd.read_sql_query("select * from last_contact_info", con=engine)

VIP_Summary_f = VIP_Summary_1.merge(help_desk_info, left_on = 'email', right_on = 'requester_email' , how = 'left')

VIP_Summary_f.drop(['index','email','requester_email'], axis=1, inplace = True)

date = dt.datetime.today()-  timedelta(1)
date_1 = date.strftime("%m-%d-%Y")
filename = f'VIP_Customer_Segments_{date_1}.xlsx'

with pd.ExcelWriter(filename) as writer:
    VIP_Summary_f.reset_index(drop=True).to_excel(writer, sheet_name="VIP_Summary",index=False)


sub = f'VIP_Customer_Segments - {date_1}'

#!/usr/bin/python
import smtplib,ssl
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import formatdate
from email import encoders

def send_mail(send_from,send_to,subject,text,server,port,username='',password=''):
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = ', '.join(recipients)
    msg['Date'] = formatdate(localtime = True)
    msg['Subject'] = subject
    msg.attach(MIMEText(text))

    part = MIMEBase('application', "octet-stream")
    part.set_payload(open(filename, "rb").read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', f'attachment; filename={filename}')
    msg.attach(part)

    #context = ssl.SSLContext(ssl.PROTOCOL_SSLv3)
    #SSL connection only working on Python 3+
    smtp = smtplib.SMTP_SSL(server, port)
    smtp.login(username,password)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.quit()
    
subject = sub
body = f"Hi,\n\n Attached contains list of VIP customer Segments as of {date_1}\n\nThanks,\nSaketh"
sender = "sakethg250@gmail.com"
recipients = ["saketh@crystalwg.com","alberto@crystalwg.com",\
             "isaac@crystalwg.com","ron@crystalwg.com","sebastian@crystalwg.com",\
             "rafael@crystalwg.com","sandra@crystalwg.com","ximena@crystalwg.com","erika@crystalwg.com","camila@crystalwg.com"]
password = "ztrgzuxuzwbgfskf"
send_mail(sender, recipients, subject, body, "smtp.gmail.com", 465,sender,password)
