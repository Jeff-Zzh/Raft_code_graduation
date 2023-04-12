import sqlite3

conn = sqlite3.connect('../DB/test.db')
cur = conn.cursor()  # 游标cursor对象，该对象的.execute()方法可以执行sql命令，进行数据操作

# 查询数学成绩大于90分的学生
sql_text_3 = "SELECT * FROM scores WHERE 数学>90"
cur.execute(sql_text_3)
# 获取查询结果
res1 = cur.fetchall()
print(res1)

resall = cur.execute('SELECT * FROM scores')
rows = cur.fetchall()
print(rows)
for row in rows:
    print(row, type(row))