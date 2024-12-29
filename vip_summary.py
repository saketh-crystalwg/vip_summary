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

engine = create_engine('postgresql://u24oms6hlf95tc:pc754b964184cc5affbc1d688ffa420767bb6f85f7f24f759c924b3fd125d46dd@c6m929eht211hc.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com/d5t2ohqpj43jns',\
                           echo = False)
                           

vip_cust_details = pd.read_sql_query('''with  dpst_rlp  as  ( \
select  "ClientId", "UserName", date("CreationTime") as txn_date, (CURRENT_DATE - date("CreationTime")) as date_diff_dpst, \
sum("ConvertedAmount") as dpst_amt, count("Id") as dpst_cnt, \
SUM(SUM("ConvertedAmount")) OVER (PARTITION BY "ClientId" ORDER BY date("CreationTime")) AS cumulative_dpst_amt, \
    SUM(COUNT(DISTINCT "Id")) OVER (PARTITION BY "ClientId" ORDER BY date("CreationTime")) AS cumulative_dpst_count \
from customer_transactions_betfoxx \
where "Status" in ('Approved','ApprovedManually') \
and "Type" =  2 \
group  by 1,2,3,4), \
\

vip as (select "ClientId",  min("txn_date") as vip_date from dpst_rlp \
    where cumulative_dpst_amt >= 200 or cumulative_dpst_count >= 5 \
    group  by  1), \

wtdrl_rlp  as  ( \
select  "ClientId", "UserName", date("CreationTime") as txn_date, (CURRENT_DATE - date("CreationTime")) as date_diff_wtdrl, \
sum("ConvertedAmount") as wtdrl_amt, count("Id") as wtdrl_cnt \
from customer_transactions_betfoxx \
where "Status" in ('Approved','ApprovedManually') \
and "Type" =  1 \
group  by 1,2,3,4), \
\
dpst_f as ( \
select "ClientId", sum(case when date_diff_dpst <= 7 then dpst_amt else 0  end ) as deposit_7_days, \
sum(case when date_diff_dpst <= 14 then dpst_amt else 0  end ) as deposit_14_days, \
sum(case when date_diff_dpst <= 21 then dpst_amt else 0  end ) as deposit_21_days, \
sum(case when date_diff_dpst <= 32 then dpst_amt else 0  end ) as deposit_32_days, \
sum(case when date_diff_dpst <= 60 then dpst_amt else 0  end ) as deposit_60_days, \
sum(case when date_diff_dpst <= 90 then dpst_amt else 0  end ) as deposit_90_days, \
sum(dpst_amt) as deposit_lifetime, \
sum(dpst_cnt) as total_deposits_count, \
min(date_diff_dpst) as days_since_last_deposit , \
max(date_diff_dpst) as days_since_first_deposit \
from dpst_rlp \
group by 1), \
\
wtdrl_f as ( \
select "ClientId", sum(case when date_diff_wtdrl <= 7 then wtdrl_amt else 0  end ) as Withdrawl_7_Days, \
sum(case when date_diff_wtdrl <= 14 then wtdrl_amt else 0  end ) as Withdrawl_14_Days, \
sum(case when date_diff_wtdrl <= 21 then wtdrl_amt else 0  end ) as Withdrawl_21_Days, \
sum(case when date_diff_wtdrl <= 32 then wtdrl_amt else 0  end ) as Withdrawl_32_Days, \
sum(case when date_diff_wtdrl <= 60 then wtdrl_amt else 0  end ) as Withdrawl_60_Days, \
sum(case when date_diff_wtdrl <= 90 then wtdrl_amt else 0  end ) as Withdrawl_90_Days, \
sum(wtdrl_amt) as Withdrawl_lifetime, \
sum(wtdrl_cnt) as withdrawl_count \
from wtdrl_rlp \
group by 1), \
\
rev_base as ( \
select *,  \
CASE WHEN "CurrencyId"  = 'CAD'  then 0.67 \
WHEN "CurrencyId"  = 'USDT'  then 0.92 \
WHEN "CurrencyId"  = 'BRL'  then 0.17 \
WHEN "CurrencyId"  = 'EUR'  then 1 \
WHEN "CurrencyId"  = 'NZD'  then 0.60 \
WHEN "CurrencyId"  = 'USD'  then 0.91 \
WHEN "CurrencyId"  = 'AUD'  then 0.61 \
WHEN "CurrencyId"  = 'GBP'  then 1.18 \
else  1 end as conversion \
from customers_game_summaries_betfoxx), \
\
rev_base_2 as ( \
select "ClientId", "summary_day", (CURRENT_DATE - date("summary_day")) as date_diff_rev, ("TotalBetAmount") as bets_value, "TotalBetsCount",  "GGR" as  ggr_eur,	"NGR" as NGR_eur \
from rev_base), \
\
revenues_f as ( \
select "ClientId", sum(case when date_diff_rev <= 7 then NGR_eur else 0  end ) as NGR_7_Days, \
sum(case when date_diff_rev <= 14 then NGR_eur else 0  end ) as NGR_14_Days, \
sum(case when date_diff_rev <= 21 then NGR_eur else 0  end ) as NGR_21_Days, \
sum(case when date_diff_rev <= 32 then NGR_eur else 0  end ) as NGR_32_Days, \
sum(case when date_diff_rev <= 60 then NGR_eur else 0  end ) as NGR_60_Days, \
sum(case when date_diff_rev <= 90 then NGR_eur else 0  end ) as NGR_90_Days, \
sum(NGR_eur) as NGR_lifetime, \
sum(case when date_diff_rev <= 7 then ggr_eur else 0  end ) as GGR_7_Days, \
sum(case when date_diff_rev <= 14 then ggr_eur else 0  end ) as GGR_14_Days, \
sum(case when date_diff_rev <= 21 then ggr_eur else 0  end ) as GGR_21_Days, \
sum(case when date_diff_rev <= 32 then ggr_eur else 0  end ) as GGR_32_Days, \
sum(case when date_diff_rev <= 60 then ggr_eur else 0  end ) as GGR_60_Days, \
sum(case when date_diff_rev <= 90 then ggr_eur else 0  end ) as GGR_90_Days, \
sum(ggr_eur) as GGR_lifetime, \
(sum(case when date_diff_rev <= 7 then bets_value else 0  end ) / nullif(sum(case when date_diff_rev <= 7 then "TotalBetsCount" else 0  end ),0))  as Avg_Bet_7_Days, \
(sum(case when date_diff_rev <= 14 then bets_value else 0  end ) / nullif(sum(case when date_diff_rev <= 14 then "TotalBetsCount" else 0  end ),0)) as Avg_Bet_14_Days, \
(sum(case when date_diff_rev <= 21 then bets_value else 0  end ) / nullif(sum(case when date_diff_rev <= 21 then "TotalBetsCount" else 0  end ),0)) as Avg_Bet_21_Days, \
(sum(case when date_diff_rev <= 32 then bets_value else 0  end ) / nullif(sum(case when date_diff_rev <= 32 then "TotalBetsCount" else 0  end ),0)) as Avg_Bet_32_Days, \
(sum(case when date_diff_rev <= 60 then bets_value else 0  end ) / nullif(sum(case when date_diff_rev <= 60 then "TotalBetsCount" else 0  end ),0)) as Avg_Bet_60_Days, \
(sum(case when date_diff_rev <= 90 then bets_value else 0  end ) / nullif(sum(case when date_diff_rev <= 90 then "TotalBetsCount" else 0  end ),0)) as Avg_Bet_90_Days \
from rev_base_2 \
group by 1) , \
\
bet_day as ( \
select "ClientId", max(summary_day) as last_bet_date from rev_base_2 \
where "TotalBetsCount" > 0 \
group by 1) ,  \
\
game_bets_lft AS ( \
    SELECT \
        "ClientId", \
        "ProviderName", \
        "ProductName", \
        SUM("PlayedCount") AS bets \
    FROM \
        customers_bets_count_games_day_level \
    GROUP BY \
        "ClientId", \
        "ProviderName", \
        "ProductName" \
), \
ranked_products_lft AS ( \
    SELECT \
        "ClientId", \
        "ProviderName", \
        "ProductName", \
        bets, \
        ROW_NUMBER() OVER (PARTITION BY "ClientId" ORDER BY bets DESC) AS rank \
    FROM \
        game_bets_lft \
), \
\
top_game_lft as ( \
SELECT \
    "ClientId", \
    "ProviderName", \
    "ProductName", \
    bets, \
    rank \
FROM \
    ranked_products_lft \
WHERE \
    rank = 1 \
ORDER BY \
    "ClientId"), \
\
game_bets_7d AS ( \
    SELECT \
        "ClientId", \
        "ProviderName", \
        "ProductName", \
        SUM("PlayedCount") AS bets \
    FROM \
        customers_bets_count_games_day_level \
where (CURRENT_DATE - date("summary_day"))  <= 7 \
    GROUP BY \
        "ClientId", \
        "ProviderName", \
        "ProductName" \
), \
ranked_products_7d AS ( \
    SELECT \
        "ClientId", \
        "ProviderName", \
        "ProductName", \
        bets, \
        ROW_NUMBER() OVER (PARTITION BY "ClientId" ORDER BY bets DESC) AS rank \
    FROM \
        game_bets_7d \
),  \
\
top_game_7d as ( \
SELECT \
    "ClientId", \
    "ProviderName", \
    "ProductName", \
    bets, \
    rank \
FROM \
    ranked_products_7d \
WHERE \
    rank = 1 \
ORDER BY \
    "ClientId"), \
\
game_bets_30d AS ( \
    SELECT \
        "ClientId", \
        "ProviderName", \
        "ProductName", \
        SUM("PlayedCount") AS bets \
    FROM \
        customers_bets_count_games_day_level \
		where (CURRENT_DATE - date("summary_day"))  <= 30 \
    GROUP BY \
        "ClientId", \
        "ProviderName", \
        "ProductName" \
), \
ranked_products_30d AS ( \
    SELECT \
        "ClientId", \
        "ProviderName", \
        "ProductName", \
        bets, \
        ROW_NUMBER() OVER (PARTITION BY "ClientId" ORDER BY bets DESC) AS rank \
    FROM \
        game_bets_30d \
), \
\
top_game_30d as ( \
SELECT \
    "ClientId", \
    "ProviderName", \
    "ProductName", \
    bets, \
    rank \
FROM  \
    ranked_products_30d \
WHERE \
    rank = 1 \
ORDER BY \
    "ClientId"), \
\
game_bets_90d AS ( \
    SELECT \
        "ClientId", \
        "ProviderName", \
        "ProductName", \
        SUM("PlayedCount") AS bets \
    FROM \
        customers_bets_count_games_day_level \
		where (CURRENT_DATE - date("summary_day"))  <= 90 \
    GROUP BY \
        "ClientId", \
        "ProviderName", \
        "ProductName" \
), \
ranked_products_90d AS ( \
    SELECT \
        "ClientId", \
        "ProviderName", \
        "ProductName", \
        bets, \
        ROW_NUMBER() OVER (PARTITION BY "ClientId" ORDER BY bets DESC) AS rank \
    FROM \
        game_bets_90d), \
\
top_game_90d as ( \
SELECT \
    "ClientId", \
    "ProviderName", \
    "ProductName", \
    bets, \
    rank \
FROM \
    ranked_products_90d \
WHERE \
    rank = 1 \
ORDER BY \
    "ClientId") \
\
\
select   "Email","FirstName","LastName","MobileNumber","CountryName","LanguageId", "BirthDate","AffiliateId","PartnerId", \
date("CreationTime") as registration_date, (CURRENT_DATE - date("CreationTime")) as days_since_register, \
date(a."LastSessionDate") as last_login_date,\
CURRENT_DATE - DATE(a."LastSessionDate") AS days_since_last_login, \
b.*, c.Withdrawl_7_Days, c.Withdrawl_14_Days, c.Withdrawl_21_Days, c.Withdrawl_32_Days, c.Withdrawl_60_Days, c.Withdrawl_90_Days,c.Withdrawl_lifetime, \
c.withdrawl_count, d.NGR_7_Days, d.NGR_14_Days, d.NGR_21_Days, d.NGR_32_Days, d.NGR_60_Days, d.NGR_90_Days, d.GGR_7_Days, d.GGR_14_Days, d.GGR_21_Days, \
d.GGR_32_Days, d.GGR_60_Days, d.GGR_90_Days, d.Avg_Bet_7_Days, d.Avg_Bet_14_Days, d.Avg_Bet_21_Days, d.Avg_Bet_32_Days, d.Avg_Bet_60_Days, d.Avg_Bet_90_Days, \
d.GGR_lifetime, d.NGR_lifetime,(CURRENT_DATE - date(last_bet_date)) as days_since_last_bet, a."StateName", \
f."ProductName" as top_played_game_lft, f."bets" as bets_on_top_game_lft, \
g."ProductName" as top_played_game_7d, g."bets" as bets_on_top_game_7d, \
h."ProductName" as top_played_game_30d, g."bets" as bets_on_top_game_30d, \
i."ProductName" as top_played_game_90d, g."bets" as bets_on_top_game_90d, j.vip_date \
from customers_betfoxx as a \
left  join dpst_f  as b \
on a."Id" =  b."ClientId" \
left  join wtdrl_f  as c  \
on a."Id" =  c."ClientId" \
left  join revenues_f  as d  \
on a."Id" =  d."ClientId" \
left  join bet_day  as e  \
on a."Id" =  e."ClientId" \
left  join top_game_lft  as f  \
on a."Id" =  f."ClientId" \
left  join top_game_7d  as g  \
on a."Id" =  g."ClientId" \
left  join top_game_30d  as h  \
on a."Id" =  h."ClientId" \
left  join top_game_90d  as i  \
on a."Id" =  i."ClientId" \
left join vip  as  j
on a."Id" = j."ClientId"
where (deposit_lifetime >= 200 or total_deposits_count >= 5)''',con=engine);

vip_cust_details["deposit_7_days"] = vip_cust_details["deposit_7_days"].fillna(0).astype(int).apply(lambda x: format_currency(x, currency="EUR", locale="nl_NL"))
vip_cust_details["deposit_14_days"] = vip_cust_details["deposit_14_days"].fillna(0).astype(int).apply(lambda x: format_currency(x, currency="EUR", locale="nl_NL"))
vip_cust_details["deposit_21_days"] = vip_cust_details["deposit_21_days"].fillna(0).astype(int).apply(lambda x: format_currency(x, currency="EUR", locale="nl_NL"))
vip_cust_details["deposit_32_days"] = vip_cust_details["deposit_32_days"].fillna(0).astype(int).apply(lambda x: format_currency(x, currency="EUR", locale="nl_NL"))
vip_cust_details["deposit_60_days"] = vip_cust_details["deposit_60_days"].fillna(0).astype(int).apply(lambda x: format_currency(x, currency="EUR", locale="nl_NL"))
vip_cust_details["deposit_90_days"] = vip_cust_details["deposit_90_days"].fillna(0).astype(int).apply(lambda x: format_currency(x, currency="EUR", locale="nl_NL"))
vip_cust_details["deposit_lifetime"] = vip_cust_details["deposit_lifetime"].fillna(0).astype(int).apply(lambda x: format_currency(x, currency="EUR", locale="nl_NL"))

vip_cust_details["withdrawl_7_days"] = vip_cust_details["withdrawl_7_days"].fillna(0).astype(int).apply(lambda x: format_currency(x, currency="EUR", locale="nl_NL"))
vip_cust_details["withdrawl_14_days"] = vip_cust_details["withdrawl_14_days"].fillna(0).astype(int).apply(lambda x: format_currency(x, currency="EUR", locale="nl_NL"))
vip_cust_details["withdrawl_21_days"] = vip_cust_details["withdrawl_21_days"].fillna(0).astype(int).apply(lambda x: format_currency(x, currency="EUR", locale="nl_NL"))
vip_cust_details["withdrawl_32_days"] = vip_cust_details["withdrawl_32_days"].fillna(0).astype(int).apply(lambda x: format_currency(x, currency="EUR", locale="nl_NL"))
vip_cust_details["withdrawl_60_days"] = vip_cust_details["withdrawl_60_days"].fillna(0).astype(int).apply(lambda x: format_currency(x, currency="EUR", locale="nl_NL"))
vip_cust_details["withdrawl_90_days"] = vip_cust_details["withdrawl_90_days"].fillna(0).astype(int).apply(lambda x: format_currency(x, currency="EUR", locale="nl_NL"))
vip_cust_details["withdrawl_lifetime"] = vip_cust_details["withdrawl_lifetime"].fillna(0).astype(int).apply(lambda x: format_currency(x, currency="EUR", locale="nl_NL"))

vip_cust_details["ngr_7_days"] = vip_cust_details["ngr_7_days"].fillna(0).astype(int).apply(lambda x: format_currency(x, currency="EUR", locale="nl_NL"))
vip_cust_details["ngr_14_days"] = vip_cust_details["ngr_14_days"].fillna(0).astype(int).apply(lambda x: format_currency(x, currency="EUR", locale="nl_NL"))
vip_cust_details["ngr_21_days"] = vip_cust_details["ngr_21_days"].fillna(0).astype(int).apply(lambda x: format_currency(x, currency="EUR", locale="nl_NL"))
vip_cust_details["ngr_32_days"] = vip_cust_details["ngr_32_days"].fillna(0).astype(int).apply(lambda x: format_currency(x, currency="EUR", locale="nl_NL"))
vip_cust_details["ngr_60_days"] = vip_cust_details["ngr_60_days"].fillna(0).astype(int).apply(lambda x: format_currency(x, currency="EUR", locale="nl_NL"))
vip_cust_details["ngr_90_days"] = vip_cust_details["ngr_90_days"].fillna(0).astype(int).apply(lambda x: format_currency(x, currency="EUR", locale="nl_NL"))
vip_cust_details["ngr_lifetime"] = vip_cust_details["ngr_lifetime"].fillna(0).astype(int).apply(lambda x: format_currency(x, currency="EUR", locale="nl_NL"))

vip_cust_details["ggr_7_days"] = vip_cust_details["ggr_7_days"].fillna(0).astype(int).apply(lambda x: format_currency(x, currency="EUR", locale="nl_NL"))
vip_cust_details["ggr_14_days"] = vip_cust_details["ggr_14_days"].fillna(0).astype(int).apply(lambda x: format_currency(x, currency="EUR", locale="nl_NL"))
vip_cust_details["ggr_21_days"] = vip_cust_details["ggr_21_days"].fillna(0).astype(int).apply(lambda x: format_currency(x, currency="EUR", locale="nl_NL"))
vip_cust_details["ggr_32_days"] = vip_cust_details["ggr_32_days"].fillna(0).astype(int).apply(lambda x: format_currency(x, currency="EUR", locale="nl_NL"))
vip_cust_details["ggr_60_days"] = vip_cust_details["ggr_60_days"].fillna(0).astype(int).apply(lambda x: format_currency(x, currency="EUR", locale="nl_NL"))
vip_cust_details["ggr_90_days"] = vip_cust_details["ggr_90_days"].fillna(0).astype(int).apply(lambda x: format_currency(x, currency="EUR", locale="nl_NL"))
vip_cust_details["ggr_lifetime"] = vip_cust_details["ggr_lifetime"].fillna(0).astype(int).apply(lambda x: format_currency(x, currency="EUR", locale="nl_NL"))

vip_cust_details['avg_bet_7_days'] = vip_cust_details['avg_bet_7_days'].round(2)
vip_cust_details['avg_bet_14_days'] = vip_cust_details['avg_bet_14_days'].round(2)
vip_cust_details['avg_bet_21_days'] = vip_cust_details['avg_bet_21_days'].round(2)
vip_cust_details['avg_bet_32_days'] = vip_cust_details['avg_bet_32_days'].round(2)
vip_cust_details['avg_bet_60_days'] = vip_cust_details['avg_bet_60_days'].round(2)
vip_cust_details['avg_bet_90_days'] = vip_cust_details['avg_bet_90_days'].round(2)


date = dt.datetime.today()-  timedelta(1)
date_1 = date.strftime("%m-%d-%Y")
filename = f'Betfoxx_VIP_Cucstomers_{date_1}.xlsx'
sub = f'Betfoxx_VIP_Customers_{date_1}'

with pd.ExcelWriter(filename, engine='openpyxl') as writer:
    vip_cust_details.to_excel(writer, sheet_name="VIP_Customer_Details", index=False)


subject = sub
body = f"Hi,\n\n Attached contains the VIP Customers summary for the {date_1} for Betfoxx \n\nThanks,\nSaketh"
sender = "sakethg24@gmail.com"
recipients = ["lina@crystalwg.com","sandra@crystalwg.com","camila@crystalwg.com","isaac@crystalwg.com","sebastian@crystalwg.com","sakethg250@gmail.com","saketh@crystalwg.com"]
password = "cgtk gurq gdul ftuf"
send_mail(sender, recipients, subject, body, "smtp.gmail.com", 465, sender, password, filename)