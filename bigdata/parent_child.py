import pymysql

# 需求说明：给定父子关系，如 101-102  102-103  103-104，现要求将父下的所有自身，子都与父建立联系
# 以上数据将变化成：  101-101 101-102 101-103 101-104 102-102 102-103 102-104 103-103 103-104
#    要注意脏数据，比如101-101（自身-自身）  104-101（闭环）

DB_ADDRESS = "localhost"
USER = "root"
PASSWORD = "root"
DB_NAME = "test2"
PORT = 3306
TABLE1 = "parent_child_relation"
TABLE2 = "parent_child_relation_all"


def get_all_data(data_dict):
    db = pymysql.connect(host=DB_ADDRESS, user=USER,
                         password=PASSWORD, db=DB_NAME, port=PORT)
    cur = db.cursor()
    sql = 'select from_id,to_id from ' + TABLE1
    try:
        cur.execute(sql)
        results = cur.fetchall()
        # 遍历结果
        for row in results:
            row0 = str(row[0])
            row1 = str(row[1])
            # 去掉自身的情况
            if row0 == row1:
                continue
            if row0 in data_dict.keys():
                data_dict[row0] += "," + row1
            else:
                data_dict.update({row0: row1})
    except Exception as e:
        print(sql, '::::', str(e))


def get_all_from(data_dict):
    froms = []
    for k in data_dict.keys():
        froms.append(k)
    return froms


def find_child(id):
    if data_dict.get(id):
        return data_dict.get(id).split(",")
    return ""


def find_ids(id, ids):
    if id is '':
        return
    else:
        ids.append(id)
        tmp = find_child(id)
    for id2 in tmp:
        try:
            find_ids(id2, ids)
        except Exception as e:
            print("find_ids::::" + str(ids) + str(e))


def write_to_mysql(data_to_save):
    db = pymysql.connect(host=DB_ADDRESS, user=USER,
                         password=PASSWORD, db=DB_NAME, port=PORT)
    cur = db.cursor()
    sql = 'insert into ' + TABLE2 + " ( from_id,to_id ) values"
    i = 0
    for k, v in data_to_save.items():
        if len(v.split(",")) < 2:
            continue
        else:
            for value in v.split(","):
                sql += "(" + k + "," + value + "),"
                i += 1
    sql = sql[0:-1]
    print('items::::', i)
    try:
        cur.execute(sql)
        db.commit()
        db.close()
    except Exception as e:
        print(sql, '::::', str(e))


def truncate_table(name):
    db = pymysql.connect(host=DB_ADDRESS, user=USER,
                         password=PASSWORD, db=DB_NAME, port=PORT)
    cur = db.cursor()
    sql = 'truncate table ' + name
    try:
        cur.execute(sql)
        db.commit()
        db.close()
    except Exception as e:
        print(sql, '::::', str(e))


if __name__ == "__main__":
    truncate_table(TABLE2)
    # 获取所有数据 格式为： 父：子1，子2，子3...
    data_dict = {}
    get_all_data(data_dict)
    print("data_dict::::" + str(data_dict))
    # 获取所有父
    froms = get_all_from(data_dict)
    print("froms::::" + str(froms))
    # 根据父查找所有子孙
    data_to_save = {}
    for id in froms:
        data_to_save.update({id: ""})
        ids = []
        find_ids(id, ids)
        data_to_save.update({id: str(ids)[1:-1]})
    print("data_to_save::::", str(data_to_save))
    # 往数据库里面存数据
    write_to_mysql(data_to_save)
    print('0-0')
