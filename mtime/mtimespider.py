# -*- coding: utf-8 -*-

import json
import re
from multiprocessing.pool import ThreadPool
from urllib import urlencode
import time
import pymongo
import sys
from bs4 import BeautifulSoup
import requests
reload(sys)
sys.setdefaultencoding('utf-8')

client = pymongo.MongoClient( 'localhost', connect=False)
db = client['mtime']
mtime_data = db['mtime']
mtime_url = db['mtime_url']

def index_html(headers,index):
    data = {
        'Ajax_CallBack':'true',
        'Ajax_CallBackType':'Mtime.Channel.Pages.SearchService',
        'Ajax_CallBackMethod':'SearchPersonByPersonRegion',
        'Ajax_CrossDomain':'1',
        'Ajax_RequestUrl':'http://movie.mtime.com/people/search/focus/#sortType=4&listType=0&r=1&pageIndex=1',
        't':'201792514521846386',
        'Ajax_CallBackArgument0':'',
        'Ajax_CallBackArgument1':'0',
        'Ajax_CallBackArgument2':'1',
        'Ajax_CallBackArgument3':'4',
        'Ajax_CallBackArgument4':index,
        'Ajax_CallBackArgument5':'0'
    }
    params = urlencode(data)
    url = 'http://service.channel.mtime.com/service/search.mcs'+ '?'+ params
    try:
        s = requests.session()
        r = s.get(url,headers=headers)
        text = r.text
        com = re.compile(r'<a title=\\".*?\\" target=\\"_blank\\" href=\\"(.*?)\\"> .*?</a>',re.S)
        links = com.findall(text)
        for link in links:
            yield link
    except:
        print '第%s页出错'%index

def text_html(url,headers):
    try:
        if mtime_url.find_one({'首页': url}):
            print '%s爬过' % url
        else:
            s = requests.session()
            r = s.get(url,headers=headers)
            time.sleep(2)
            soup = BeautifulSoup(r.text,'lxml')
            names = soup.find_all('h2')
            ennames = soup.select(' p.enname')
            personjobs = soup.select(' div.per_header > p.mt9.__r_c_')
            details = soup.select('#personNavigationRegion > dd:nth-of-type(1) > a')
            filmographies = soup.select('#personNavigationRegion > dd:nth-of-type(2) > a')
            for name,enname,personjob,detail,filmographie in zip(names,ennames,personjobs,details,filmographies):
                name = name.get_text(),
                enname = enname.get_text(),
                personjob = personjob.get_text(),
                data = {
                    '名字':name,
                    '英文名字':enname,
                    '职业':personjob,
                }
                link = {
                    "首页":url
                }
                print name[0]
                mtime_data.insert(data)
                detail = detail.get('href')
                mtime_url.insert(link)
                yield detail

    except :
        print '有问题'

def detail_html(url,headers):
    try:
        s = requests.session()
        r = s.get(url, headers=headers)
        r.encoding = r.apparent_encoding
        time.sleep(2)
        text = r.text
        text = re.sub(u'教育背景：','',text)
        text = re.sub(u'家庭成员：','',text)

        soup = BeautifulSoup(text, 'lxml')
        birthdays = soup.select(' div.per_info_l > dl > dt:nth-of-type(1)')
        statures = soup.select(' div.per_info_l > dl > dt:nth-of-type(2)')
        weights = soup.select(' div.per_info_l > dl > dt:nth-of-type(3)')
        constellations = soup.select(' div.per_info_l > dl > dt:nth-of-type(4)')
        blood_types = soup.select(' div.per_info_l > dl > dt:nth-of-type(5)')
        biographys = soup.select(' #lblAllGraphy')
        for birthday,stature,weight,constellation,blood_type,biography in zip(birthdays,statures,weights,constellations,blood_types,biographys):
            profile_data = {
                '出生日期':birthday.get_text(),
                '身高':stature.get_text(),
                '体重':weight.get_text(),
                '星座':constellation.get_text(),
                '血型':blood_type.get_text(),
                '人物小传':biography.get_text()
            }
            mtime_data.update({}, {'$set': {'个人档案': profile_data}}, multi=True) #在原有的数据中添加新的数据
    except :
        print '又出错'

def main(index):
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.8',
        'Connection': 'keep-alive',
        # 'Cookie': '_userCode_=2017925111125434',
        'Referer': 'http://movie.mtime.com/people/search/focus/',
        'Upgrade - Insecure - Requests':'1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.86 Safari/537.36'
    }
    for url in index_html(headers,index):
        for link in text_html(url,headers):
            detail_html(link,headers)

if __name__ == '__main__':
    pool = ThreadPool(4)  # 指定线程数
    groups = ([x for x in range(1, 461)])  # 构建多线程
    pool.map(main, groups)
    pool.close()
    pool.join()
