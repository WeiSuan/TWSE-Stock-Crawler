from dateutil import rrule
from datetime import timedelta, datetime
import numpy
import pandas
import pickle
import csv

import os
import sys
import logging

logging.basicConfig(
    filename="D://Manual_Packages/logging/CrawlerIntegrate.log",
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
)


def logging_file(func):
    def wrapper(*arg):
        if len(arg) > 1:
            if arg[0]["stat"] == "OK":
                temp_stock = arg[0]["title"].split(" ")[1]
                logging.info(
                    "Type 0 => StockNo : {} | Date : {} | Status : {}".format(
                        temp_stock, arg[0]["date"], arg[0]["stat"]
                    )
                )
            else:
                logging.warning("Type 0 => Status : No Information")

        elif len(arg) <= 1:
            if arg[0]["stat"] == "OK":
                logging.info(
                    "Type 1/2 => Date : {} | Status : {}".format(
                        arg[0]["date"], arg[0]["stat"]
                    )
                )
            else:
                temp_json = (
                    "No Match Information"
                    if arg[0]["stat"] == "很抱歉，沒有符合條件的資料!"
                    else "Other Information"
                )
                logging.warning("Type 1/2 => Status : {}".format(temp_json))
        return func(*arg)

    return wrapper


# Type0 Integrate
# Loading the last recoding date for the un-input date


def Data_Information_Loading(stockNo_):

    lt = max(
        map(
            lambda x: x[:6],
            os.listdir("D://StockProgramming/Python_Crawler/Strike_Price/" + stockNo_),
        )
    )
    with open(
        "D://StockProgramming/Python_Crawler/Strike_Price/"
        + stockNo_
        + "/"
        + lt
        + ".pickle",
        "rb",
    ) as file_:

        temp_data = pickle.load(file_)

    if temp_data["stat"] == "很抱歉，沒有符合條件的資料!":
        return None
    else:

        temp_data = temp_data["data"]

        lt = str(
            int(max([item for item in zip(*temp_data)][0]).replace("/", "")) + 19110000
        )
        return lt


# Loading hte last recoding date for the input date


def Data_Price_Loading(stockNo_):

    last_date = pandas.read_csv(
        "D://StockProgramming/Price/" + stockNo_ + ".csv", usecols=["Date"], dtype="str"
    )["Date"].max()
    return last_date


# Data Preprocessing


@logging_file
def Data_Transfer_Type0(temp_dict, lt_in):

    # temp_dict : Source data loading with dictionary type
    # lt_in : the last recording date for input data

    if temp_dict["stat"] == "OK":

        temp_data = temp_dict["data"]
        temp_final = list()
        for item_sort, item_content in enumerate(zip(*temp_data)):
            if item_sort == 0:
                temp_final.append(
                    list(
                        map(lambda x: int(x.replace("/", "")) + 19110000, item_content)
                    )
                )
                continue
            temp_final.append(
                list(
                    map(
                        lambda x: "NaN"
                        if x in ["X0.00", "--"]
                        else float(x.replace(",", "")),
                        item_content,
                    )
                )
            )

        temp_argmax = numpy.argmax([item > int(lt_in) for item in temp_final[0]])
        temp_final = [main_item[temp_argmax:] for main_item in temp_final]
        temp_final = [list(i) for i in zip(*temp_final)]
        return temp_final
    else:
        return None


# Type1 Integrate(Date / Total Amount / Securities Investment Trust / Dealer / Foreign Investor)


@logging_file
def Data_Transfer_Type1(temp_dict):
    if temp_dict["stat"] == "OK":

        temp_date = temp_dict["date"]
        temp_data = [temp_dict["fields"]] + temp_dict["data"]

        temp_data = [list(item) for item in zip(*temp_data)]

        # Length 12 : < 20141201 / Length 16 : 20141201 <= < 20171218 / Length 19 : >= 20171218
        # Only need to seperate name of foreign investor  : (1) < 20171218 catch name called as '外資買賣超股數'
        #                                                   (2) >= 20171218 catch name called as '外陸資買賣超股數(不含外資自營商)'
        temp_final = list()

        for i in temp_data:
            if i[0] in ["證券代號", "三大法人買賣超股數", "投信買賣超股數", "自營商買賣超股數"]:
                temp_final.append(i)
            elif (i[0] == "外資買賣超股數") & (temp_date < "20171218"):
                temp_final.append(i)
            elif (i[0] == "外陸資買賣超股數(不含外資自營商)") & (temp_date >= "20171218"):
                temp_final.append(i)
            else:
                continue

        temp_final = sorted(temp_final, reverse=True)
        temp_final_2 = [[int(temp_date)] * len(temp_final[0][1:]), temp_final[0][1:]]
        for item in temp_final[1:]:
            temp_final_2.append(
                list(map(lambda x: float(x.replace(",", "")), item[1:]))
            )

        return [list(item) for item in zip(*temp_final_2)]
    else:
        return None


# Type2 Integrate


@logging_file
def Data_Transfer_Type2(temp_dict):
    if temp_dict["stat"] == "OK":
        temp_date, temp_data = (
            temp_dict["date"],
            [temp_dict["fields"]] + temp_dict["data"],
        )
        temp_data = [list(item) for item in zip(*temp_data)]

        temp_final = list()
        for i in temp_data:
            if i[0] in ["證券代號", "本益比", "殖利率(%)", "股價淨值比"]:
                temp_final.append(i)
            else:
                continue
        temp_final = sorted(temp_final, reverse=True)
        temp_final_2 = [
            [int(temp_date)] * len(temp_final[0][1:]),
            [str(item) for item in temp_final[0][1:]],
        ]

        def simple_transfer(x):
            try:
                return float(x.replace(",", ""))
            except:
                return "NaN"

        for item in temp_final[1:]:
            temp_final_2.append(list(map(lambda x: simple_transfer(x), item[1:])))

        return [list(item) for item in zip(*temp_final_2)]
    else:
        return None


def Source_Data(type_, stockNo_=None):

    if type_ == 0:

        if stockNo_ is None:
            logging.error("Without input stock number !!")
            raise TypeError

        if stockNo_ not in [
            item.split(".csv")[0] for item in os.listdir("D://StockProgramming/Price/")
        ]:
            lt_in, lt_un = "20150101", Data_Information_Loading(stockNo_)
            pandas.DataFrame(
                columns=[
                    "Date",
                    "Trade_Volumn",
                    "Trade_Amount",
                    "Open",
                    "High",
                    "Low",
                    "Close",
                    "Change",
                    "Transaction",
                ]
            ).to_csv("D://StockProgramming/Price/" + stockNo_ + ".csv", index=False)

        else:
            lt_in, lt_un = (
                Data_Price_Loading(stockNo_),
                Data_Information_Loading(stockNo_),
            )

        if lt_in == lt_un:
            logging.info(
                "Type 0(Same Input Date) => StockNo : {} | Date : {}".format(
                    stockNo_, lt_in
                )
            )
            return None

        if lt_un is None:
            logging.info(
                "Type 0(Without Current Data) => StockNo : {} | Date : {}".format(
                    stockNo_, lt_in
                )
            )
            return None

        with open(
            "D://StockProgramming/Price/" + stockNo_ + ".csv", "a+", newline=""
        ) as file_csv:
            for sub_month in rrule.rrule(
                rrule.MONTHLY,
                dtstart=datetime.strptime(lt_in[:6], "%Y%m"),
                until=datetime.strptime(lt_un[:6], "%Y%m"),
            ):
                sub_month = sub_month.strftime("%Y%m")
                with open(
                    "D://StockProgramming/Python_Crawler/Strike_Price/"
                    + stockNo_
                    + "/"
                    + sub_month
                    + ".pickle",
                    "rb",
                ) as file_pickle:
                    temp_pickle = pickle.load(file_pickle)

                temp_pickle = Data_Transfer_Type0(temp_pickle, lt_in)
                if temp_pickle is not None:
                    csv.writer(file_csv).writerows(temp_pickle)

    elif type_ == 1:
        with open(
            "D://StockProgramming/Python_Crawler/DailyInformationType1.txt", "r"
        ) as file_txt:
            lt_in, lt_un = file_txt.read().split("\n")[-3:-1]

        if lt_in == lt_un:
            print("You have the same date with two inputs date")
            return None

        if lt_un is None:
            print("You have the same date with two inputs date")
            return None
        # lt_in, lt_un = "20200916", "20210618"

        for sub_day in rrule.rrule(
            rrule.DAILY,
            dtstart=datetime.strptime(lt_in, "%Y%m%d") + timedelta(days=1),
            until=datetime.strptime(lt_un, "%Y%m%d"),
        ):
            sub_day = sub_day.strftime("%Y%m%d")
            with open(
                "D://StockProgramming/Python_Crawler/Institute_Investment/All/"
                + sub_day
                + ".pickle",
                "rb",
            ) as file_pickle:
                temp_pickle = pickle.load(file_pickle)

            temp_pickle = Data_Transfer_Type1(temp_pickle)

            if temp_pickle is not None:
                pandas.DataFrame(
                    temp_pickle, columns=["Date", "Stock", "D", "SIT", "FI", "Total"]
                ).to_csv(
                    "D://StockProgramming/Volume/" + sub_day + ".csv",
                    index=False,
                    sep=",",
                    line_terminator="\n",
                )
                # pandas.DataFrame(
                #     temp_pickle, columns=["Date", "Stock", "Total", "FI", "SIT", "D"]
                # ).to_csv("D://StockProgramming/Volume/" + sub_day + ".csv", index=False)
    elif type_ == 2:
        with open(
            "D://StockProgramming/Python_Crawler/DailyInformationType2.txt", "r"
        ) as file_txt:
            lt_in, lt_un = file_txt.read().split("\n")[-3:-1]

        with open(
            "D://StockProgramming/Others/Others.csv", "a+", newline=""
        ) as file_csv:
            for sub_day in rrule.rrule(
                rrule.DAILY,
                dtstart=datetime.strptime(lt_in, "%Y%m%d") + timedelta(days=1),
                until=datetime.strptime(lt_un, "%Y%m%d"),
            ):
                sub_day = sub_day.strftime("%Y%m%d")
                with open(
                    "D://StockProgramming/Python_Crawler/Others/" + sub_day + ".pickle",
                    "rb",
                ) as file_pickle:
                    temp_pickle = pickle.load(file_pickle)

                temp_pickle = Data_Transfer_Type2(temp_pickle)
                if temp_pickle is not None:
                    pandas.DataFrame(
                        temp_pickle, columns=["Date", "Stock", "PE", "DY", "PB"]
                    ).to_csv(
                        "D://StockProgramming/Others/" + sub_day + ".csv", index=False
                    )

    else:
        logging.error("No this option !!")
        raise TypeError


if __name__ == "__main__":

    temp_list = os.listdir("D://StockProgramming/Python_Crawler/Strike_Price")
    for sub_stockNo_ in temp_list:
        Source_Data(type_=0, stockNo_=sub_stockNo_)

    Source_Data(type_=1, stockNo_=None)
    Source_Data(type_=2, stockNo_=None)
