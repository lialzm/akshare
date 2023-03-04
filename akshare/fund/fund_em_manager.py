#!/usr/bin/env python
# -*- coding:utf-8 -*-

import time
import json

from akshare.utils import demjson
import pandas as pd
import requests
from lxml import etree
import datetime
import time
import pytz as pytz
import xlwt
from datetime import date


def fund_manager_fund_info_em(fund: str = "000009") -> pd.DataFrame:
    """
    东方财富网-天天基金网-基金数据-货币型基金收益-历史净值数据
    http://fundf10.eastmoney.com/jjjl_000001.html
    :param fund: 货币型基金代码, 可以通过 fund_money_fund_daily_em 来获取
    :type fund: str
    :return: 东方财富网站-天天基金网-基金数据-货币型基金收益-历史净值数据
    :rtype: pandas.DataFrame
    """
    url = f"http://fundf10.eastmoney.com/jjjl_{fund}.html"
    print(url)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36 Edg/108.0.1462.76",
        "Referer": "http://fundf10.eastmoney.com/"
    }
    r = requests.get(url, headers=headers)
    html = etree.HTML(r.text)

    tr = html.xpath('//table[@class="w782 comm  jloff"]/tbody/tr')
    row = 1
    result = []
    for item in tr:
        start_time = html.xpath('//table[@class="w782 comm  jloff"]/tbody/tr[{}]/td[1]/text()'.format(row))
        end_time = html.xpath('//table[@class="w782 comm  jloff"]/tbody/tr[{}]/td[2]/text()'.format(row))
        manager = html.xpath('//table[@class="w782 comm  jloff"]/tbody/tr[{}]/td[3]/a'.format(row))
        total_time = html.xpath('//table[@class="w782 comm  jloff"]/tbody/tr[{}]/td[4]/text()'.format(row))
        return_data = html.xpath('//table[@class="w782 comm  jloff"]/tbody/tr[{}]/td[5]/text()'.format(row))
        tz = pytz.timezone('Asia/Shanghai')  # 东八区
        t = datetime.datetime.fromtimestamp(int(time.time()), tz).strftime('%Y-%m-%d %H:%M:%S %z')

        for text in manager:
            res_tr = []
            res_tr.append(start_time[0])

            if len(end_time) > 0:
                if end_time[0] == '至今':
                    end_time[0] = None
                res_tr.append(end_time[0])
            else:
                res_tr.append(None)

            res_tr.append(text.text)

            try:
                res_tr.append(text.attrib['href'].replace('http://fund.eastmoney.com/manager/', '').replace('.html', ''))
            except Exception as e:
                res_tr.append('')

            if len(total_time) > 0:
                res_tr.append(total_time[0])
            else:
                res_tr.append('')

            if len(return_data) > 0:
                if return_data[0] != '--':
                    res_tr.append(return_data[0].replace('%',''))
                else:
                    res_tr.append(0)
            else:
                res_tr.append(0)
            res_tr.append(t)
            res_tr.append(t)
            res_tr.append(fund)
            result.append(res_tr)
        row += 1

    data = pd.DataFrame(result,columns=['start_time', 'end_time', 'manager', 'manager_id', 'total_time', 'total_return',
                                 'dw_insert_time', 'dw_update_time','ts_code'])

    return data


def fund_manager_det_fund_info_em(fund: str = "30532593") -> pd.DataFrame:
    url = f"http://fund.eastmoney.com/manager/{fund}.html"
    print(url)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36",
        "Referer": f"http://fund.eastmoney.com/manager/{fund}.html"
    }
    r = requests.get(url, headers=headers)
    r.encoding = "utf-8"
    html = etree.HTML(r.text)
    name = html.xpath('//h3[@id="name_1"]/text()')
    picture = html.xpath('//img[@id="photo"]/@src')
    total_time = html.xpath('string(//div[@class="right jd "])').replace(' ', '').replace('\r\n', '')
    total_time_start = total_time.index('累计任职时间：')
    total_time_end = total_time.index('任职起始日期：')
    total_time_res = total_time[total_time_start:total_time_end].replace('累计任职时间：', '')
    start_time_start = total_time.index('任职起始日期：')
    start_time_end = total_time.index('现任基金公司：')
    start_time_res = total_time[start_time_start:start_time_end].replace('任职起始日期：', '')
    profile = html.xpath('string(//div[@class="right ms"]/p)').replace(' ', '').replace('\r\n', '')

    tz = pytz.timezone('Asia/Shanghai')  # 东八区
    t = datetime.datetime.fromtimestamp(int(time.time()), tz).strftime('%Y-%m-%d %H:%M:%S %z')

    result = {'name': name[0], 'id': fund, 'picture': picture[0], 'total_time': total_time_res,'start_time': start_time_res, 'profile': profile, 'dw_insert_time': t, 'dw_update_time': t}
    data = pd.DataFrame([result], columns=['name', 'id', 'picture', 'total_time', 'start_time', 'profile', 'dw_insert_time', 'dw_update_time'])
    return data

def fund_rank_list(start_time: str="2020-01-01", end_time: str="2020-01-31", page: int=1):
    url = f"http://fund.eastmoney.com/data/rankhandler.aspx?op=dy&dt=kf&ft=zq&rs=&gs=0&sc=qjzf&st=desc&sd={start_time}&ed={end_time}&es=1&qdii=&pi={page}&pn=50&dx=0&v=0.767176542309342"
    header = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "Connection": "keep-alive",
        "Cookie": "qgqp_b_id=f97d2cbe42b8bbae80ec68f1e3b2140a; st_pvi=94755241731394; st_sp=2023-01-02%2015%3A53%3A34; st_inirUrl=http%3A%2F%2Ffundf10.eastmoney.com%2F; ASP.NET_SessionId=lfme1bbnoxicpuhp4wav4g3s",
        "Host": "fund.eastmoney.com",
        "Referer": "http://fund.eastmoney.com/data/diyfundranking.html",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36 Edg/110.0.1587.57"
    }

    r = requests.get(url=url,headers=header)
    result_text = r.text.replace('var rankData = ' ,'').replace(';' ,'')\
        .replace('datas','"datas"')\
        .replace('allRecords','"allRecords"')\
        .replace('pageIndex','"pageIndex"')\
        .replace('pageNum','"pageNum"')\
        .replace('allPages','"allPages"')\
        .replace('allNum','"allNum"')\
        .replace('gpNum','"gpNum"')\
        .replace('hhNum','"hhNum"')\
        .replace('zqNum','"zqNum"')\
        .replace('zsNum','"zsNum"')\
        .replace('bbNum','"bbNum"')\
        .replace('qdiiNum','"qdiiNum"')\
        .replace('etfNum','"etfNum"')\
        .replace('lofNum','"lofNum"')

    result = {'datas':[],'allPages':0}
    list = json.loads(result_text)
    if len(list['datas']) == 0:
        return []

    result['allPages'] = list['allPages']

    for item in list['datas']:
        item = item.split(',')
        print(item)
        result_item = {'基金代码':item[0],'基金简称':item[1],'期间涨幅':item[3],'期间分红(元/份)':item[4],'分红次数':item[5],'起始日期':item[6],'单位净值':item[7],'累计净值':item[8],'终止日期':item[9],'单位净值2':item[10],'累计净值2':item[11],'成立日期':item[12],'手续费':item[14]}
        result['datas'].append(result_item)
    return result

def doFundRankList():
    workbook = xlwt.Workbook()
    sheet1 = workbook.add_sheet('sheet1', cell_overwrite_ok=True)  # 创建表单
    sheet1.write(0, 0, '基金代码')
    sheet1.write(0, 1, '基金简称')
    sheet1.write(0, 2, '期间涨幅')
    sheet1.write(0, 3, '期间分红(元/份)')
    sheet1.write(0, 4, '分红次数')
    sheet1.write(0, 5, '起始日期')
    sheet1.write(0, 6, '单位净值')
    sheet1.write(0, 7, '累计净值')
    sheet1.write(0, 8, '终止日期')
    sheet1.write(0, 9, '单位净值')
    sheet1.write(0, 10, '累计净值')
    sheet1.write(0, 11, '成立日期')
    sheet1.write(0, 12, '手续费')
    sheet1.write(0, 13, '序号')
    sheet1.write(0, 14, '当前年月')


    row = 1

    result = []

    for year in range(2020, 2024):
        for month in range(1, 13):
            start_date = datetime.date(year, month, 1)
            if month == 12:
                end_date = datetime.date(year, month, 31)
            else:
                end_date = datetime.date(year, month + 1, 1) - datetime.timedelta(1)



            serial = 1
            pageNum = 0
            for i in range(1,150):
                if pageNum != 0 and i > pageNum:
                    continue

                data = fund_rank_list(start_date,end_date,i)
                pageNum = data['allPages']
                print(str(pageNum)+'总页数')
                for item in data['datas']:
                    sheet1.write(row, 0, item['基金代码'])
                    sheet1.write(row, 1, item['基金简称'])
                    sheet1.write(row, 2, item['期间涨幅'])
                    sheet1.write(row, 3, item['期间分红(元/份)'])
                    sheet1.write(row, 4, item['分红次数'])
                    sheet1.write(row, 5, item['起始日期'])
                    sheet1.write(row, 6, item['单位净值'])
                    sheet1.write(row, 7, item['累计净值'])
                    sheet1.write(row, 8, item['终止日期'])
                    sheet1.write(row, 9, item['单位净值2'])
                    sheet1.write(row, 10, item['累计净值2'])
                    sheet1.write(row, 11, item['成立日期'])
                    sheet1.write(row, 12, item['手续费'])
                    sheet1.write(row, 13, serial)
                    if int(month) < 10:
                        sheet1.write(row, 14, str(year) + '-0' + str(month))
                    else:
                        sheet1.write(row, 14, str(year) + '-' + str(month))
                    row += 1
                    serial+=1
                time.sleep(2)
                workbook.save('信息.xlsx')


if __name__ == "__main__":
    # fund_money_fund_info_em_df = fund_manager_fund_info_em(fund="002005")
    # print(fund_money_fund_info_em_df)
    # fund_manager_det_fund_info_em = fund_manager_det_fund_info_em(fund="30532593")
    # print(fund_manager_det_fund_info_em)
    doFundRankList()
