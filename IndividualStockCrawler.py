import datetime

import requests
import json


def stock_html_preprocess(html_) : 

    html_data = [['NaN' if sub_item in ["X0.00", "--"] else sub_item.replace(',', '') for sub_item in item] for item in html_['data']]

    data_ = pd.DataFrame(html_data)
    data_.columns = ["Date", "Volume", "Amount", "Open_Price", "High_Price", "Low_Price", "Close_Price", "Change_Price","Transaction"]


    data_['Date'] = [datetime.date(int(item_.split('/')[0]) + 1911, int(item_.split('/')[1]), int(item_.split('/')[2])) for item_ in data_.Date]
    data_['Volume'] = data_.Volume.astype('int64')
    data_['Amount'] = data_.Amount.astype('int64')

    data_['Open_Price'] = data_.Open_Price.astype('float64')
    data_['High_Price'] = data_.High_Price.astype('float64')
    data_['Low_Price'] = data_.Low_Price.astype('float64')
    data_['Close_Price'] = data_.Close_Price.astype('float64')
    data_['Change_Price'] = data_.Change_Price.astype('float64')

    return data_

    
def stock_html_UnitMonth(stock_, date_ = datetime.datetime.now().date().strftime('%Y%m%d')) : 

    url_ = ("https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date=" \
    + "replace_element"
    + "&stockNo="
    + stock_)

    # 轉成html格式
    res_ = requests.get(url_)
    html_ = json.loads(res_.text)
    
    # 轉成DataFrame
    return stock_html_preprocess(html_)
    