import pymysql.cursors
import sys
# 定义从字典-SQL变换服务
def insert(base, table, dt):
    # 连接数据库，创建游标
    db = pymysql.connect(host='192.168.11.128',
                         user='root',
                         password='root',
                         db=base,
                         charset='utf8mb4',
                         cursorclass=pymysql.cursors.DictCursor)
    cursor = db.cursor()
    # 创建占位符并动态生成行列式
    placeholder = ", ".join(["%s"] * len(dt))
    stmt = "insert into `{table}` ({columns}) values ({values});".format(table=table, columns=",".join(dt.keys()),
                                                                         values=placeholder)
    cursor.execute(stmt, list(dt.values()))
    db.commit()
    cursor.close()
    db.close()


# 连接新数据库服务
connection_new = pymysql.connect(host='192.168.11.128',
                                 user='root',
                                 password='root',
                                 db='management',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

# 连接旧数据库服务
connection_old = pymysql.connect(host='192.168.11.128',
                                 user='root',
                                 password='root',
                                 db='management_old',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
with connection_new.cursor() as cursor_new:
    sql = "show tables"
    cursor_new.execute(sql)
    result = cursor_new.fetchall()
    # 重组新数据库集合服务
    new_db_tables = []
    for x in result:
        new_db_tables.append(x['Tables_in_management'])

with connection_old.cursor() as cursor_old:
    sql = "show tables"
    cursor_old.execute(sql)
    result = cursor_old.fetchall()
    old_db_tables = []
    for x in result:
        old_db_tables.append(x['Tables_in_management_old'])

    for table in old_db_tables:
        if table not in new_db_tables:
            # 获取旧数据库中不在新数据库的表的建表语句服务
            sql_create = "SHOW CREATE TABLE " + table
            cursor_old.execute(sql_create)
            create_old_tables = cursor_old.fetchone()

            # 在新的数据库中执行建表语句并填充数据服务
            cursor_new_create = connection_new.cursor()
            cursor_new_create.execute(str(create_old_tables['Create Table']))
            connection_new.commit()

            cursor_old.execute("select * from " + table)
            datas_old_tables = list(cursor_old.fetchall())
            for sql_disc in datas_old_tables:
                insert("management", table, sql_disc)
            print('ok')
            sys.exit()
