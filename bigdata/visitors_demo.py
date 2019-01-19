import pymysql  # 导入 bigdata


# 获取所有数据
def find_all_data():
    db = pymysql.connect(host="localhost", user="root",
                         password="root", db="test2", port=3306)
    cur = db.cursor()
    sql = "select * from visitors where date is not null"
    try:
        cur.execute(sql)
        results = cur.fetchall()  # 结果是元组
        all_data = list(results)
        return all_data
    except Exception as e:
        raise e
    finally:
        db.close()


# 清空表
def truncate_table(db_name,table_name):
    db = pymysql.connect(host="localhost", user="root",
                         password="root", db=db_name, port=3306)
    cur = db.cursor()
    sql = " truncate table "+table_name
    print('truncate table sql:  ' + sql)
    try:
        cur.execute(sql)
        db.commit()
    except Exception as e:
        raise e
    finally:
        db.close()


# 将计算出的累加数据插入 visitors_total
def insert_visitors_total(date_persons_total):
    db = pymysql.connect(host="localhost", user="root",
                         password="root", db="test2", port=3306)
    cur = db.cursor()
    # sql = "drop table if exists visitors_total ; create table visitors_total ( id int(11) primary key auto_increment,date date , person_counts_total int(11))"
    sql = " insert into visitors_total (date,person_counts_total) values"
    for k, v in date_persons_total.items():
        sql +=' (\''+ k.strftime('%Y-%m-%d') + '\',' + str(v) + '),'
    sql = sql[0:-1]
    print('insert_visitors_total sql:  '+sql)
    try:
        cur.execute(sql)
        db.commit()
    except Exception as e:
        raise e
    finally:
        db.close()


if __name__ == '__main__':
    all_data = find_all_data()
    print('all_data:  ', all_data)
    all_data1 = all_data[:]

    # 取出所有日期并去重
    all_dates = {}
    for item in all_data:
        date_need = item[2]
        all_dates.update({date_need: ""})
    print('all_dates:  ', all_dates)

    # 取出所有人并去重
    all_persons = {}
    for item in all_data:
        date_need = item[1]
        all_persons.update({date_need: ""})
    print('all_persons:  ', all_persons)

    # 统计日期和访问次数
    date_persons_counts = {}
    for k in all_dates.keys():
        count = 0
        j = 0
        for j in all_data1:
            if k == j[2]:
                count += 1
        date_persons_counts.update({k: count})
    print('date_persons_counts:  ', date_persons_counts)

    # 统计日期和人
    date_persons = {}
    for k in all_dates.keys():
        persons = []
        j = 0
        for j in all_data1:
            if k == j[2]:
                persons.append(j[1])
        date_persons.update({k: persons})
    print('date_persons:  ', date_persons)

    # 按天累加去重统计访客人数
    date_persons_total = {}
    temp_person = {}
    for k, v in date_persons.items():
        for vv in v:
            temp_person.update({vv: ''})
        date_persons_total.update({k: temp_person.__len__()})
    print('date_persons_total', date_persons_total)
    truncate_table('test2','visitors_total')
    insert_visitors_total(date_persons_total)