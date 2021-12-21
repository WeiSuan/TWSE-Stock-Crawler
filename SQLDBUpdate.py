import mysql.connector
import os
import pandas as pd

import datetime
from sqlalchemy import create_engine

# from tqdm import tqdm, trange

import logging

logging.basicConfig(
    filename="C://Users/WeiSuan/Desktop/Coding/mysql_err.log",
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
)

# db_ = mysql.connector.connect(host = 'localhost', user = 'root', passwd = '123456789', database = 'STOCK_PREVIOUS')
# db_cur = db_.cursor()

# # Create Price Table
# statement_create_price_table = """
# create table stock_price (
# Date date not NULL,
# Stock varchar(20) not NULL,
# Volume bigint unsigned,
# Amount bigint unsigned,
# Open_Price float(9, 4) unsigned,
# High_Price float(9, 4) unsigned,
# Low_Price float(9, 4) unsigned,
# Close_Price float(9, 4) unsigned,
# Change_Price float(9, 4),
# Transaction bigint unsigned,
# primary key(Stock asc, Date asc)
# )
# """
# db_cur.execute(statement_create_price_table)
# db_.commit()

# from sqlalchemy import create_engine
# engine = create_engine("mysql+pymysql://root:123456789@localhost:0000/STOCK_PREVIOUS")

# list_ = os.listdir('D://StockProgramming/Price/')
# temp_print = None
# for sub_csv in list_ :

#     temp_ = pd.read_csv('D://StockProgramming/Price/' + sub_csv)
#     temp_.columns = ["Date", "Volume", "Amount", "Open_Price", "High_Price", "Low_Price", "Close_Price", "Change_Price", "Transaction"]
#     temp_ = temp_.drop_duplicates(subset = 'Date').sort_values(by = 'Date', ascending = False).assign(Stock = sub_csv[:4]).set_index(['Date', 'Stock'])

#     temp_.to_sql(con = engine, name = 'stock_price', if_exists = 'append', index = True)

##############################################################################################################################################################

# db_ = mysql.connector.connect(host = 'localhost', user = 'root', passwd = '123456789', database = 'STOCK_PREVIOUS')
# db_cur = db_.cursor()

# state_ = """
# create table stock_volume (
# Stock varchar(20) not NULL,
# Date date not NULL,
# D bigint,
# SIT bigint,
# FI bigint,
# Total bigint,
# primary key(Stock asc, Date desc)
# )"""
# db_cur.execute(state_)
# db_.commit()

# ?????????DB

# from sqlalchemy import create_engine
# engine = create_engine("mysql+pymysql://root:123456789@localhost:0000/STOCK_PREVIOUS")

# list_ = os.listdir('D://StockProgramming/Volume/')
# temp_print = None
# for sub_csv in list_ :

#     if temp_print is None :
#         temp_print = sub_csv[:6]
#         print(temp_print)

#     if sub_csv[:6] != temp_print :
#        temp_print = sub_csv[:6]
#        print(temp_print)

#     temp_ = pd.read_csv('D://StockProgramming/Volume/' + sub_csv)
#     temp_.columns = ['Date', 'Stock', 'D', 'SIT', 'FI', 'Total']
#     temp_ = temp_.sort_values(by = ['Date', 'Stock'], ascending = [False, True]). set_index(['Date', 'Stock'])

#     temp_.to_sql(con = engine, name='stock_volume', if_exists = 'append', index = True)

##############################################################################################################################################################
def mysql_price_update():

    db_ = mysql.connector.connect(
        host="localhost", user="root", passwd="123456789", database="STOCK_PREVIOUS"
    )
    db_cur = db_.cursor()
    engine = create_engine(
        "mysql+pymysql://root:123456789@localhost:0000/STOCK_PREVIOUS"
    )

    list_ = os.listdir("D://StockProgramming/Price/")
    for sub_csv in list_:

        state_ = """ select max(Date) as max_ from stock_price where stock = '{}'""".format(
            sub_csv.split(".csv")[0]
        )
        db_cur.execute(state_)
        price_latest_date = db_cur.fetchall()[0][0]
        price_latest_date = (
            "20150101"
            if price_latest_date is None
            else price_latest_date.strftime("%Y%m%d")
        )

        if datetime.date.today().strftime("%Y%m%d") > price_latest_date:

            price_latest_date = int(price_latest_date)

            temp_ = pd.read_csv("D://StockProgramming/Price/" + sub_csv).query(
                "Date > @price_latest_date"
            )
            temp_.columns = [
                "Date",
                "Volume",
                "Amount",
                "Open_Price",
                "High_Price",
                "Low_Price",
                "Close_Price",
                "Change_Price",
                "Transaction",
            ]
            temp_ = (
                temp_.drop_duplicates(subset="Date")
                .sort_values(by="Date", ascending=False)
                .assign(Stock=sub_csv.split(".csv")[0])
                .set_index(["Date", "Stock"])
            )

            temp_.to_sql(con=engine, name="stock_price", if_exists="append", index=True)

    db_.commit()


def mysql_volume_update():

    while True:
        try:
            db_ = mysql.connector.connect(
                host="localhost",
                user="root",
                passwd="123456789",
                database="STOCK_PREVIOUS",
            )
            db_cur = db_.cursor()

            state_ = """ select max(Date) as max_ from stock_volume """
            db_cur.execute(state_)
            volume_latest_date = db_cur.fetchall()[0][0].strftime("%Y%m%d")
            db_.commit()
            break
        except:
            continue

    list_ = [
        item
        for item in os.listdir("D://StockProgramming/Volume/")
        if item.split(".csv")[0] > volume_latest_date
    ]

    if len(list_) != 0:

        engine = create_engine(
            "mysql+pymysql://root:123456789@localhost:0000/STOCK_PREVIOUS"
        )
        for sub_csv in list_:
            temp_ = pd.read_csv("D://StockProgramming/Volume/" + sub_csv)

            # temp_.columns = ["Date", "Stock", "D", "SIT", "FI", "Total"]
            temp_ = temp_.sort_values(
                by=["Date", "Stock"], ascending=[False, True]
            ).set_index(["Date", "Stock"])

            temp_.to_sql(
                con=engine, name="stock_volume", if_exists="append", index=True
            )


##############################################################################################################################################################
def mysql_merge_update():

    while True:

        try:
            db_ = mysql.connector.connect(
                host="localhost",
                user="root",
                passwd="123456789",
                database="STOCK_PREVIOUS",
            )

            db_cur = db_.cursor()
            db_cur.execute("""drop table if exists stock_main""")
            state_ = """
                create table stock_main as 
                select tb1.Date, tb1.Stock,
                    round(Volume / 1000) as Volume,
                    round(Amount / 1000) as Amount,
                    Open_Price, High_Price, Low_Price, Close_Price, Change_Price, Transaction,
                    round(D / 1000) as Dealer,
                    round(SIT / 1000) as SIT,
                    round(FI / 1000) as FI,
                    round(Total / 1000) as Total3
                    from stock_price tb1
                left join stock_volume tb2
                    on tb1.stock = tb2.stock and tb1.date = tb2.date
                    """

            db_cur.execute(state_)
            db_.commit()
            break

        except:
            continue


if __name__ == "__main__":
    mysql_price_update()
    mysql_volume_update()
    mysql_merge_update()
