

import pymysql  # 导入 bigdata

# 打开数据库连接
db = pymysql.connect(host="localhost", user="root",
                     password="root", db="test2", port=3306)

# 使用cursor()方法获取操作游标
cur = db.cursor()

# 1.查询操作
# 编写sql 查询语句  user 对应我的表名
sql = "select * from tbl_user"
try:
    cur.execute(sql)  # 执行sql语句
    results = cur.fetchall()  # 获取查询的所有记录
    print("id", "name", "password")
    # 遍历结果
    for row in results:
        id = row[0]
        name = row[1]
        password = row[2]
        print(id, name, password)
except Exception as e:
    raise e




# 2.插入操作
sql_insert = """insert into tbl_user (name,password) values('liu','1234')"""
try:
    cur.execute(sql_insert)
    # 提交
    db.commit()
except Exception as e:
    # 错误回滚
    db.rollback()




# 3.更新操作
sql_update = "update tbl_user set name = '%s' where id = %d"

try:
    cur.execute(sql_update % ("xiongda", 5))  # 像sql语句传递参数
    # 提交
    db.commit()
except Exception as e:
    # 错误回滚
    db.rollback()



# 4.删除操作
sql_delete = "delete from tbl_user where id = %d"

try:
    cur.execute(sql_delete % (6))  # 像sql语句传递参数
    # 提交
    db.commit()
except Exception as e:
    # 错误回滚
    db.rollback()
finally:
    db.close()






















