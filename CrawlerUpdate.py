import requests
import json
import pickle
from json.decoder import JSONDecodeError

import sys
import os
import time

import pandas as pd
import numpy as np

from dateutil import rrule
from datetime import datetime

import logging

# Create a table shows that rows denote all of company, columns denote recoding date and elemens in this table
# mean the lastest recording date(If something abnormal we need to make a note.)

logging.basicConfig(
    filename="D://Manual_Packages/logging/Crawler.log",
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
)


def DailyLoading(path_):
    if os.path.isfile(path_):
        temp_data = pd.read_csv(path_, dtype="str")

        ct = datetime.strftime(datetime.now(), "%Y%m%d")

        if ct not in temp_data.columns:
            temp_data[ct] = ["NoRecord"] * temp_data.shape[0]
            temp_data.to_csv(
                "D://StockProgramming/Python_Crawler/DailyInformationType0.csv",
                index=False,
                quoting=1,
            )

        return temp_data
    else:
        print("Can't loading the specfic file.")
        raise TypeError


def DailyGet(last_time):
    current_time = datetime.now()
    daily_time = rrule.rrule(
        rrule.DAILY,
        dtstart=datetime.strptime(last_time, "%Y%m%d"),
        until=datetime.now(),
    )
    return [datetime.strftime(item, "%Y%m%d") for item in daily_time][1:]


def DailyCrawler(type_, stockNo_):

    if type_ == 0:

        if stockNo_ is None:
            print("Type 0 : without input stock number")
            raise TypeError

        temp_data = pd.read_csv(
            "D://StockProgramming/Python_Crawler/DailyInformationType0.csv", dtype="str"
        )
        if any(temp_data.StockNo.str.contains(stockNo_)):
            last_time = temp_data.Date.loc[
                temp_data.StockNo.str.contains(stockNo_)
            ].iloc[0]

        # temp_data = DailyLoading(
        #     path_="D://StockProgramming/Python_Crawler/DailyInformationType0.csv")
        # temp_ = temp_data.loc[temp_data.Index.isin(
        #     [stockNo_]), :].values.reshape([-1])
        # for item in np.flip(temp_, 0):
        #     if item != 'NoRecord':
        #         last_time = item
        #         break

    elif type_ == 1:
        with open(
            "D://StockProgramming/Python_Crawler/DailyInformationType1.txt", "r"
        ) as file_:
            last_time = file_.read().split("\n")[-2]
    elif type_ == 2:
        with open(
            "D://StockProgramming/Python_Crawler/DailyInformationType2.txt", "r"
        ) as file_:
            last_time = file_.read().split("\n")[-2]
    else:
        print("No option for this parameter !!")
        raise TypeError

    return DailyGet(last_time=last_time)


def DailyWrite(type_, stockNo_):
    ct = datetime.now().strftime("%Y%m%d")
    if type_ == 0:
        temp_data = pd.read_csv(
            "D://StockProgramming/Python_Crawler/DailyInformationType0.csv", dtype="str"
        )
        temp_data.loc[temp_data.StockNo == stockNo_, "Date"] = ct
        temp_data.to_csv(
            "D://StockProgramming/Python_Crawler/DailyInformationType0.csv",
            index=False,
            quoting=1,
        )
    elif type_ == 1:
        with open(
            "D://StockProgramming/Python_Crawler/DailyInformationType1.txt", "a+"
        ) as file_:
            file_.write(ct + "\n")
    else:
        with open(
            "D://StockProgramming/Python_Crawler/DailyInformationType2.txt", "a+"
        ) as file_:
            file_.write(ct + "\n")


# Daily Information / Monthly per table


def Table_Type(temp_all_date_, type_):
    if type_ == 0:
        dt_m = pd.unique([item[:6] for item in temp_all_date_])
        temp_result = list()
        for item in dt_m:
            temp_result.append(
                list(filter(lambda x: x[:6] == item, temp_all_date_))[-1]
            )
        return temp_result
    elif type_ in [1, 2]:
        return temp_all_date_
    else:
        print("No option for this parmeter!!!")
        raise TypeError


def Url_Path_Type(type_, stockNo_):
    if type_ == 0:

        if stockNo_ is None:
            print("Type 0 : without input stock number")
            raise TypeError

        url_ = (
            "https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date="
            + "replace_element"
            + "&stockNo="
            + stockNo_
        )
        path_ = "D://StockProgramming/Python_Crawler/Strike_Price/" + stockNo_
    elif type_ == 1:
        url_ = (
            "https://www.twse.com.tw/fund/T86?response=json&date="
            + "replace_element"
            + "&selectType=ALL"
        )
        path_ = "D://StockProgramming/Python_Crawler/Institute_Investment/All"
    elif type_ == 2:
        url_ = (
            "https://www.twse.com.tw/exchangeReport/BWIBBU_d?response=json&date="
            + "replace_element"
            + "&selectType=ALL"
        )
        path_ = "D://StockProgramming/Python_Crawler/Others"
    else:
        print("No option for this parameter !!")
        raise TypeError

    return url_, path_


class Crawler:
    @staticmethod
    def get_html(date_, url_, type_, timesleep_=2.5):

        requests.adapters.DEFAULT_RETRIES = 5
        temp_res = requests.session()
        temp_res.keep_alive = False

        res_ = None
        error_num = 1

        timeout_ = (0.5, 0.5) if type_ == 0 else (0.5, 3)

        while res_ is None:
            try:
                res_ = requests.get(url_, timeout=timeout_)
                time.sleep(timesleep_)

            except Exception as e:
                print(
                    "Time : {time} | Error(II) : {content}".format(
                        time=datetime.now().strftime("%Y%m%d %H:%M:%S"),
                        content="JSONDecodeError",
                    )
                )

                if error_num == 3:
                    print(
                        "Can't catch any information on {time}".format(
                            time=datetime.now().strftime("%Y%m%d %H:%M:%S")
                        )
                    )
                    break

                error_num = error_num + 1
                # detect_error(e)
                time.sleep(60)
                continue

        return res_ if res_ is None else json.loads(res_.text)

    @staticmethod
    def save_html(temp_json, name_, path_):

        if os.path.exists(path_) == False:
            os.makedirs(path_)

        with open(path_ + "/" + name_ + ".pickle", "wb") as file_:
            pickle.dump(temp_json, file_)

    @classmethod
    def BaseInformation(cls, type_, stockNo_=None):

        # type_ : select what kinds of information we wanna catch
        #         (1) 0 : Monthly
        #         (2) 1 : Daily

        temp_all_date = DailyCrawler(type_, stockNo_)
        temp_all_date = Table_Type(temp_all_date_=temp_all_date, type_=type_)

        url_, path_ = Url_Path_Type(type_=type_, stockNo_=stockNo_)
        for each_date_ in temp_all_date:
            url_new = url_.replace("replace_element", each_date_)
            # Resolving html into json file
            temp_json = cls.get_html(date_=each_date_, url_=url_new, type_=type_)

            while None is None:
                temp_json = cls.get_html(date_=each_date_, url_=url_new, type_=type_)
                if (temp_json["stat"] == "OK") or (
                    temp_json["stat"] == "很抱歉，沒有符合條件的資料!"
                ):
                    break
                else:
                    continue

            temp_stat = (
                temp_json["stat"]
                if temp_json["stat"] == "OK"
                else "No Match Information"
            )

            # Saving as pickle file
            cls.save_html(
                temp_json=temp_json,
                name_=each_date_[:6] if type_ == 0 else each_date_,
                path_=path_,
            )

            logging.info("Date : {} | Status : {}".format(each_date_, temp_stat))

        DailyWrite(type_, stockNo_)
        logging.info("DailyConfigureing Successful !!\n\n")


if __name__ == "__main__":

    # Type 0
    temp_list = os.listdir("D://StockProgramming/Python_Crawler/Strike_Price")

    for sub_stockNo_ in temp_list:
        logging.info("{}===================================>".format(sub_stockNo_))
        Crawler.BaseInformation(type_=0, stockNo_=sub_stockNo_)

    # Type 1
    logging.info("Type 1 : Starting !")
    Crawler.BaseInformation(type_=1, stockNo_=None)

    # Type 2
    logging.info("Type2 : Starting !")
    Crawler.BaseInformation(type_=2, stockNo_=None)

