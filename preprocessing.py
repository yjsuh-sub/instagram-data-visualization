
# coding: utf-8

# # Preprocessing code

# ### Necessary packages


import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pylab as plt
import pytz
from glob import glob
from os import getcwd
import sys


# ### Useful functions


# dataframe 속에 시간 항목을 datetime 데이터형으로 바꿔서 저장하는 함수
# boolean이 True인 경우 NaN 항목을 데이터프레임에서 삭제
# Set local timezone
seoultz = pytz.timezone('Asia/Seoul')

# 각 포스트 당 테그 수를 합산하여 테그수 열을 추가하는 함수
def num_tags(df_name):
    count = 0
    df_name['number of tags'] = np.zeros_like(df_name['uid'])
    
    for i, dtitem in enumerate(df_name['tags']):
        try:
            df_name['number of tags'][i] = dtitem.count('#')
        except:
            if str(dtitem).lower() == 'nan':
                df_name['number of tags'][i] = 0
            else:
                count += 1
    
    if count:
        print('Number of fails: %s'%count)
    else:
        print('success')

# tags 중 tag_name 언급 횟수를 기록하여 col_name 열을 추가하는 함수
def suyo_tags(df_name, tag_name, col_name):
    count = 0
    df_name[col_name] = np.zeros_like(df_name['uid'])
    
    for i, dtitem in enumerate(df_name['tags']):
        try:
            df_name[col_name][i] = dtitem.count(tag_name)
        except:
            if str(dtitem).lower() == 'nan':
                df_name[col_name][i] = 0
            else:
                count += 1
    
    if count:
        print("Number of fails: %s"%count)
        
# str 속성을 가진 정수를 int 속성으로 바꾸는 함수
def int_maker(df_name, col_name):
    count = 0
    
    for i, dtitem in enumerate(df_name[col_name]):
        try:
            df_name[col_name][i] = int(dtitem.replace(',', ''))
        except:
            if str(dtitem).lower() == 'nan':
                df_name[col_name][i] = int(0)
            else:
                count += 1
    
    if count:
        print("Number of fails: %s"%count)
        
# 같은 열 데이터 중 다른 데이터형이 있는지 확인하는 함수
def type_tester(df_name, col_name, tp):
    count = 0
    
    for dtitem in df_name[col_name]:
        if type(dtitem) != tp:
            count += 1
    
    if count:
        print("Number of different types: %s"%count)        

# 테그 : 빈도수 사전 리턴
def tag_analyze(df_name):
    test_list = []
    for item in df_name['tags']:
        if type(item) == str:
            test_list.append(item)
            #print(item)

    vect = CountVectorizer()
    vect.fit(test_list)
    voc_dic = vect.vocabulary_.items()
    voc_len = len(vect.transform(test_list).toarray()[0])
    number = np.zeros(voc_len)

    for np_item in vect.transform(test_list).toarray():
        number += np_item

    new_dic = {}
    for key, val in voc_dic:
        new_dic[key] = number[val]
    return new_dic

# 데이터프레임에 테그 리스트 열 추가
def add_tag_list(df_name):
    df_name['tag list'] = np.zeros_like(df_name['uid'])
    for i, t_item in enumerate(df_name['tags']):
        try:
            df_name['tag list'][i] = t_item.split('#')[1:]
            #print(t_item.split('#')[1:])
        except:
            df_name['tag list'][i] = []

# user id eraser
def eraser(t):
    usrlist = {}
    i = 0
    for item in t.split('>'):
        if ('<' in item) & (item.split('<')[-1] not in usrlist.keys()):
            i += 1
            usrlist[item.split('<')[1]] = 'user%s'%i
    for key, value in usrlist.iteritems():
        t = t.replace(key, value)
    return t


# ### Importing crawling file


def dataframe(path=getcwd(), name=""):
    data_dic = {}
    all_path = glob('%s/*.csv' % path)
    if name:
        try:
            all_path = glob('%s/%s' % (path, name))
        except:
            pass

    for p in all_path:
        dt = pd.read_csv('%s'%p)
        dt = dt.drop(['uid', 'account'], axis=1) # Drop unnecessary column
        data_dic[p.split('_')[1].split('.')[0]] = dt
    return data_dic

# ### Replace user id to arbitrary in comment

def irs(dic, erase=True):
    if erase:
        for res in dic.keys():
            for i in range(len(dic[res].comments)):
                try:
                    dic[res].comments[i] = eraser(dic[res].comments[i])
                except:
                    pass
    return dic

# ### Changing timezone from Utc to Seoul(local timezone)

def timezone(dic, local_tz=seoultz):
    for key in ddic.keys():
        df1 = dic[key]
        df1.datetime = pd.to_datetime(df1.datetime)
        df1 = df1.set_index('datetime').tz_convert(local_tz, level=0).tz_convert(None)
        df1 = df1.reset_index()
        dic[key] = df1
    return dic


# ### Add columns: week(weekday) and hour(24h)

def add_wh(dic):
    for key in dic.keys():
        dic[key]['week'] = dic[key].datetime.dt.dayofweek #0:Mon ... 6: Sun
        dic[key]['hour'] = dic[key].datetime.dt.hour
    return dic

# ### Add location
def add_location(dic):
    df0 = dic[dic.keys()[0]]
    df0['location'] = np.full_like(df0['datetime'].astype(str), dic.keys()[0])
    for key in dic.keys():
        if key == dic.keys()[0]:
            pass
        else:
            df1 = dic[key]
            df1['location'] = np.full_like(df1.datetime.astype(str), key)
        df0 = pd.concat([df0, df1])
    return dic

# ### Save dataframe to .csv file

def save(dic, name='insta_final', path=getcwd()):
    df0.to_csv('{0}{1}.csv'.format([path, name]), sep=',')

# ### Read csv file


if __name__ == "__main__":
    if len(sys.argv) > 0:
        path = sys.argv[1]
        if len(sys.argv) > 1:
            name = sys.argv[2]
    else:
        path = getcwd()
    dic = dataframe(path)
    irs(dic)
    timezone(dic)
    add_wh(dic)
    add_location(dic)
    save(dic)

