#!/usr/bin/python 
# coding=utf-8

import time
import datetime
import redis
import urllib2
import ConfigParser

class Group:
  total_number = 0
  def __init__(self, chargename, phone, \
               suffixflag=0, suffix_db=0, suffix_period=0, suffix_table=[], \
               nosuffixflag=0, nosuffix_db=0, nosuffix_period=0, nosuffix_table=[]):
    self.chargename = chargename
    self.phone = phone
    if suffixflag:
      self.suffixflag = suffixflag
      self.suffix_db = suffix_db
      self.suffix_period = suffix_period
      self.suffix_table = []
      for each in suffix_table: 
        self.suffix_table.append(each)
      if not nosuffixflag:
        self.nosuffixflag = 0

    if nosuffixflag:
      self.nosuffixflag = nosuffixflag
      self.nosuffix_db = nosuffix_db
      self.nosuffix_period = nosuffix_period
      self.nosuffix_table = []
      for each in nosuffix_table:  
        self.nosuffix_table.append(each)
      if not suffixflag:
        self.suffixflag = 0

  def SetTime(self):
    time ='init_time'
    if self.suffixflag:  # 有后缀
      if self.suffix_period == 24:  # 每天
        time = GetYesterday()
      elif self.suffix_period == 1: # 每小时
        time = GetLastTwoHour()
      elif self.suffix_period == 7: # 每周一
        time = GetLastMonday()
      elif self.suffix_period == 30: # 每月
        time = GetLastMonthFristDay()
      elif self.suffix_period == 5:  # 交易日
        time = GetToday()

      self.check_suffix_table = []
      for each in self.suffix_table: 
        self.check_suffix_table.append(each + str(time))

    if self.nosuffixflag: # 无后缀
      self.check_nosuffix_table = []
      for each in self.nosuffix_table:
        self.check_nosuffix_table.append(each)

  def CheckList(self):
    self.SetTime()
    self.errorlist = []    # 更新失误列表
    if self.suffixflag:    # 有后缀
      r = connect_redis(self.suffix_db)
      for eachtable in self.check_suffix_table:
        ret = r.exists(eachtable)
        if ret == False:
          self.AddErrorKey(eachtable)
    if self.nosuffixflag:  # 无后缀
      r = connect_redis(self.nosuffix_db)
      for eachtable in self.check_nosuffix_table:
        ret = r.exists(eachtable)
        if ret == False:
          self.AddErrorKey(eachtable)
        correcttime = float(GetToday(1))
        if 'over_or_under_weight_holding' == eachtable:
          date = r.hget(eachtable, "last_update_date")
          date = float(date)
        else:   
          date = r.zscore(eachtable, "last_update_date")
        if correcttime != date:
          self.AddErrorKey(eachtable)

  def AddErrorKey(self, errorkey):
    self.errorlist.append(errorkey)

def connect_redis(redis_db):
  cp = ConfigParser.ConfigParser()
  cp.read("./redis.conf")
  redis_host = cp.get("redis", "host")
  redis_port = cp.getint("redis", "port")
  redis_pwd = cp.get("redis", "pwd")
  pool = redis.ConnectionPool(host = redis_host, port = redis_port, db = redis_db, password = redis_pwd)
  r = redis.Redis(connection_pool = pool)
  return r

def GetToday(timeflag=0):
  today = datetime.date.today()
  if not timeflag:
    return today.strftime('%Y-%m-%d')
  else:
    return today.strftime('%Y%m%d') 

def GetYesterday(timeflag=0):
    # 获取前一天年月日，按格式要求拼接
    # timeflag = 0 时，2016-11-01格式，默认
    # timeflag = 1 时，20161101格式
  today = datetime.date.today() 
  oneday = datetime.timedelta(days=1) 
  yesterday = today - oneday  
  return yesterday.strftime('%Y-%m-%d') 

def GetLastTwoHour():
  # 获得上两小时时间
  r = time.strftime('%Y-%m-%d-%H', time.localtime(time.time()-7200))
  return r

def GetLastMonday():
  # 获得上周第一天日期
  today = datetime.date.today()  
  curtime = time.localtime(time.time())
  oneday = datetime.timedelta(days = 1)  
  lastMonday = today - (7 + curtime.tm_wday) * oneday 
  return lastMonday.strftime('%Y-%m-%d')

def GetLastMonthFristDay():
  # 获得上个月第一天 年月日
  day = (datetime.date.today().replace(day=1) - datetime.timedelta(1)).replace(day=1)
  return str(day)

def send_sms(recv_name, recv_tel_number, error_list, period):
  message = '%s您好,' % recv_name + '%s如下数据没有检测成功:' % period
  count = 0
  list_length = len(error_list)
  if error_list:
    for item in error_list:
      message = message + item
      count = count + 1
      if count < list_length:
        message = message + ','
  else:
    message = '%s您好,' % recv_name + '%s数据更新正常。' % period
  print message
  url = 'http://smsapi.c123.cn/OpenPlatform/OpenApi?action=sendOnce&ac=1001@501318590001&authkey=C4545CC1AE91802D2C0FBA7075ADA972&cgid=52&csid=50131859&c=' + message + '&m=' + recv_tel_number
  print url
  print time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
  req = urllib2.Request(url)
  res_data = urllib2.urlopen(req)
  res = res_data.read()
  print res

def is_work_day():
  date = GetToday()
  date = date.replace('-', '')
  cp = ConfigParser.ConfigParser()
  cp.read("./is_work_day.txt")
  is_work_day = cp.get("is_work_day", date)
  return is_work_day

def check_group():
  structure_prefix = ['change_percent_', 'amplitude_', 'turnoverratio_',  \
                      'volume_', 'turnover_', 'share_price_', 'inc_change_percent_', \
                      'dec_change_percent_', 'absolute_change_percent_']
  indicator_noprefix = ['total_equity', 'float_equity', 'market_value', \
                        'liquid_market_value', 'liquid_scale', 'top_stock_ratio', \
                        'holder_count', 'float_stock_num', 'change_in_holding',  \
                        'institution_stock_num', 'over_or_under_weight_holding']
  structure_group = Group('刘宏伟', '18621532630', 
                          1, 15, 5, structure_prefix,
                          1, 15, 5, indicator_noprefix)

  heat_prefix = ['count_heat_', 'diff_heat_']
  heat_group = Group('司建省', '17839613797', \
                     1, 0, 24, heat_prefix)

  announcement_prefix = ['announcement_illegal_', 'announcement_increase_', \
                         'announcement_lawsuit_', 'announcement_profit_']
  announcement_group = Group('张鑫', '15397919751', 1, 0, 24, announcement_prefix)

  # 暂时不需检查
  # vipstock_prefix = ['vipstockstatistic_rise_', 'vipstockstatistic_down_']
  # vipstock_group = Group('李煜', '18516255516', 1, 10, 24, vipstock_prefix)

  datafrom_prefix = ['moer_vipstockstatistic_rise_', 'moer_vipstockstatistic_down_', \
                     'xueqiu_vipstockstatistic_rise_','xueqiu_vipstockstatistic_down_', \
                     'zhongjin_vipstockstatistic_rise_', 'zhongjin_vipstockstatistic_down_']
  datafrom_group = Group('李煜', '18516255516', 1, 10, 1, datafrom_prefix)

  stockdaily_perfix = ['exposure_', 'trend_', 'visit_']
  stockdaily_group = Group('王草', '17705853018', 1, 0, 24, stockdaily_perfix)

  stockweek_prefix = ['exposureWeek_', 'trendWeek_', 'visitWeek_']
  stockweek_group = Group('王草', '17705853018', 1, 0, 7, stockweek_prefix)

  stockMonth_prefix = ['exposureMonth_', 'trendMonth_', 'visitMonth_']
  stockMonth_group = Group('王草', '17705853018', 1, 0, 30, stockMonth_prefix)

  senti_events_prefix = ['sentiment_', 'events_']
  senti_events_group = Group('裘虬', '18758035499', 1, 0, 24, senti_events_prefix)

  hot_prefix = ['heat_mean_5_', 'heat_mean_7_', 'heat_mean_10_', 'heat_mean_14_',  \
                'heat_mean_15_', 'heat_mean_20_', 'heat_mean_30_', 'heat_mean_60_',\
                'heat_std_5_', 'heat_std_7_', 'heat_std_10_', 'heat_std_14_',      \
                'heat_std_15_', 'heat_std_20_', 'heat_std_30_', 'heat_std_60_',    \
                'industry_heat_mean_5_', 'industry_heat_mean_7_', 'industry_heat_mean_10_', \
                'industry_heat_mean_14_', 'industry_heat_mean_15_', 'industry_heat_mean_20_',\
                'industry_heat_mean_30_', 'industry_heat_mean_60_', \
                'industry_heat_std_5_', 'industry_heat_std_7_', 'industry_heat_std_10_', 'industry_heat_std_14_',\
                'industry_heat_std_15_', 'industry_heat_std_20_', 'industry_heat_std_30_', 'industry_heat_std_60_',\
                'industry_heat_']
  hot_group = Group('杨德城', '18017872187', 1, 0, 24, hot_prefix)

  GroupHour = []
  GroupHour.append(datafrom_group)

  GroupDay = []
  GroupDay.append(heat_group)
  GroupDay.append(announcement_group)
  # GroupDay.append(vipstock_group)
  GroupDay.append(stockdaily_group)
  GroupDay.append(senti_events_group)
  GroupDay.append(hot_group)

  GroupWeek = []
  GroupWeek.append(stockweek_group)

  GroupMonth = []
  GroupMonth.append(stockMonth_group)

  GroupWorkDay = []
  GroupWorkDay.append(structure_group)

  return GroupHour, GroupDay, GroupWorkDay, GroupWeek, GroupMonth

def main():
  check_group_tuple = check_group()
  GroupHour = check_group_tuple[0]
  GroupDay = check_group_tuple[1]
  GroupWorkDay = check_group_tuple[2]
  GroupWeek = check_group_tuple[3]
  GroupMonth = check_group_tuple[4]
  while True:
    current_time = time.localtime(time.time())
    work_day = int(is_work_day())
    if current_time.tm_min == 0:
      for group in GroupHour:
        group.CheckList()
        if ((group.chargename == '李煜') and (not group.errorlist)):
          pass
        else:
          send_sms(group.chargename, group.phone, group.errorlist, '前两小时')
          time.sleep(30)
          send_sms('', '18668169052', group.errorlist, '前两小时')
        time.sleep(2) #发送短信间隔2s

    if ((current_time.tm_hour == 10) and (current_time.tm_min == 0)): #每天十点检测
      for group in GroupDay:
        group.CheckList()
        send_sms(group.chargename, group.phone, group.errorlist, '昨日')
        time.sleep(30)
        if group.errorlist:
          send_sms('', '18668169052', group.errorlist, '昨日')
        time.sleep(300) 

    if ((current_time.tm_hour == 22) and (current_time.tm_min == 0)): # 每天22点检测
      if work_day:
        for group in GroupWorkDay:
          group.CheckList()
          send_sms(group.chargename, group.phone, group.errorlist, '今日')
          time.sleep(30)
          if group.errorlist:
            send_sms('', '18668169052', group.errorlist, '今日')
          time.sleep(2)

    if ((current_time.tm_wday == 0) and (current_time.tm_hour == 9) and (current_time.tm_min == 30)): #每周一九点半检测
      for group in GroupWeek:
        group.CheckList()
        send_sms(group.chargename, group.phone, group.errorlist, '上周一')
        time.sleep(30)
        if group.errorlist:
          send_sms('', '18668169052', group.errorlist, '上周一')        
        time.sleep(2)

    if ((current_time.tm_mday == 1) and (current_time.tm_hour == 9) and (current_time.tm_min == 0)): #每月一号九点检测
      for group in GroupMonth:
        group.CheckList()
        send_sms(group.chargename, group.phone, group.errorlist, '上月一日')
        time.sleep(30)
        if group.errorlist:
          send_sms('', '18668169052', group.errorlist, '上月一日') 
        time.sleep(2)
    time.sleep(60)

main()





            
        
