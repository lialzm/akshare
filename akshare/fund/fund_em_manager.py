#!/usr/bin/env python
# -*- coding:utf-8 -*-

import time
import json

from akshare.utils import demjson
import pandas as pd
import requests
from lxml import etree

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
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36",
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
        time = html.xpath('//table[@class="w782 comm  jloff"]/tbody/tr[{}]/td[4]/text()'.format(row))
        return_data = html.xpath('//table[@class="w782 comm  jloff"]/tbody/tr[{}]/td[5]/text()'.format(row))
        for text in manager:
            res_tr = []
            res_tr.append(start_time[0])
            res_tr.append(end_time[0])
            res_tr.append(text.text)
            res_tr.append(text.attrib['href'].replace('http://fund.eastmoney.com/manager/','').replace('.html',''))
            res_tr.append(time[0])
            res_tr.append(return_data[0])
            result.append(res_tr)
        row+=1

    data = pd.DataFrame(result, columns=['start_time', 'end_time','manager','manager_id','total_time','total_return'])

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
    total_time = html.xpath('string(//div[@class="right jd "])').replace(' ','').replace('\r\n','')
    total_time_start = total_time.index('累计任职时间：')
    total_time_end = total_time.index('任职起始日期：')
    total_time_res = total_time[total_time_start:total_time_end].replace('累计任职时间：','')

    start_time_start = total_time.index('任职起始日期：')
    start_time_end = total_time.index('现任基金公司：')
    start_time_res = total_time[start_time_start:start_time_end].replace('任职起始日期：','')

    profile = html.xpath('string(//div[@class="right ms"]/p)').replace(' ','').replace('\r\n','')

    result = {'name':name[0],'id':fund,'picture':picture[0],'total_time':total_time_res,'start_time':start_time_res,'profile':profile}
    data = pd.DataFrame([result], columns=['name', 'id','picture','total_time','start_time','profile'])
    return data

if __name__ == "__main__":
    fund_money_fund_info_em_df = fund_manager_fund_info_em(fund="000001")
    print(fund_money_fund_info_em_df)
    fund_manager_det_fund_info_em = fund_manager_det_fund_info_em(fund="30532593")
    print(fund_manager_det_fund_info_em)