import sqlite3

conn = sqlite3.connect('../DB/test.db')
cur = conn.cursor()  # 游标cursor对象，该对象的.execute()方法可以执行sql命令，进行数据操作

# 删除表的sql语句
sql_text_drop = 'DROP TABLE scores'
cur.execute(sql_text_drop)

# 建表的sql语句
sql_text_1 = '''CREATE TABLE scores
           (姓名 TEXT,
            班级 TEXT,
            性别 TEXT,
            语文 NUMBER,
            数学 NUMBER,
            英语 NUMBER);'''
# 执行sql语句
cur.execute(sql_text_1)

# 插入单条数据
sql_text_2 = "INSERT INTO scores VALUES('A', '一班', '男', 96, 94, 98)"
cur.execute(sql_text_2)

# 插入多条数据
data = [
    ('B', '一班', '女', 78, 87, 85),
    ('C', '一班', '男', 98, 84, 90)
]
cur.executemany('INSERT INTO scores VALUES (?,?,?,?,?,?)', data)

# 连接完数据库并不会自动提交，所以需要手动 commit 你的改动conn.commit()
conn.commit()  # commit()后才看得到数据库中插入的多条数据

# 使用完数据库之后，需要关闭游标和连接：
# 关闭游标
cur.close()
# 关闭连接
conn.close()