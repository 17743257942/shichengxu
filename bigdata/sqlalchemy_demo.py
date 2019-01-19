
from sqlalchemy import Column, String, create_engine, Integer
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


# 创建对象的基类:
Base = declarative_base()
# 定义User对象:
class User(Base):
    # 表的名字:
    __tablename__ = 'tbl_user'
    # 表的结构:
    id = Column(Integer, primary_key=True)
    name = Column(String(128))
    password = Column(String(128))
    # 表的初始化
    def __init__(self,row):
        self.name=row[0]
        self.password=[1]

# 初始化数据库连接:'数据库类型+数据库驱动名称://用户名:口令@机器地址:端口号/数据库名'
engine = create_engine('mysql+pymysql://root:root@localhost:3306/test2')
# 创建DBSession类型:
DBSession = sessionmaker(bind=engine)
# 创建session对象:
session = DBSession()

def insert():
    # 创建新User对象:
    row = ('swagg', 'iekdkeidk')
    # 添加到session:
    session.add(User(row))
    # 提交即保存到数据库:
    session.commit()


def inserts():
    row = ('swagg', 'iekdkeidk')
    session.add(User(row))
    session.commit()

def query():
    user = session.query(User).filter_by(name='sdf').first()
    session.commit()
    if user:
        print(user.name,user.password)


def delete():
    user = session.query(User).filter_by(name="swagg").first()
    session.delete(user)
    session.commit()


def update():
    user = session.query(User).filter_by(name="qwer").first()
    user.password = "newpassword"
    session.commit()


# 关闭session:
session.close()


if __name__ =="__main__":
    # insert()
    inserts()
    # query()
    # delete()
    # update()
















