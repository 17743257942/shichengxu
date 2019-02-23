import datetime
import time  # 引入time模块
import calendar

import datetime

import datetime as datetime

ticks = time.time()
print  ("当前时间戳为:", ticks)

print('本地时间为：',time.localtime(time.time()))

localtime = time.asctime( time.localtime(time.time()) )
print ("本地时间为 :", localtime)

# 格式化成2016-03-20 11:45:39形式
print (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))



cal = calendar.month(2016, 1)
print("以下输出2016年1月份的日历:")
print(cal)

list1=[(1, 'a', datetime.date(2018, 1, 1)), (2, 'b', datetime.date(2018, 1, 2)), (3, 'c', datetime.date(2018, 1, 3)), (4, 'a', datetime.date(2018, 1, 2)), (5, 'b', datetime.date(2018, 1, 1)), (6, 'c', datetime.date(2018, 1, 2)), (7, 'd', datetime.date(2018, 1, 4))]
print(list1)
list1.remove((1, 'a', datetime.date(2018, 1, 1)))
print(list1)
for i in range(len(list1)):
    print(i)

# 时间的比较
print(datetime.date(2018, 1, 1) )
print(datetime.date(2018, 1, 1) + datetime.timedelta(days=1) > datetime.date(2018, 1, 1))

dd=datetime.date(2018, 1, 1)
dd=dd.strftime('%Y-%m-%d')
print(dd)

print('sss,'+str(33))

sth=None
print(str(sth))

v='asdf,asd,sss,ee'
print(v.split(","))


