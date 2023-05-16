import time
import os
import sqlite3
from Utils.RaftSyncedSQLiteDB import RaftSyncedSQLiteDB
os.chdir('E:\Raft')  # 工作路径切换


# 创建测试用节点和performance_test.db
test_node = RaftSyncedSQLiteDB('localhost:4321', ['localhost:4322', 'localhost:4323'], 'performance_test')
time.sleep(2)

# 创建性能测试用表 程序运行前确保系统没有该表
column_name_list = ['column1', 'column2', 'column3']
datatype_list = ['TEXT', 'INT', 'CHAR(50)']
isNULL_list = ['NOT NULL', 'NOT NULL', 'NOT NULL']
table_info = {'column': column_name_list, 'datatype': datatype_list, 'isnull': isNULL_list}
test_node.create_table('pt_tb', table_info)
test_node.close()  # 关闭本节点与数据库的连接


# 1.数据库直接写性能
conn = sqlite3.connect(r'.\DB\syncDB\performance_test.db')
cur = conn.cursor()
duration = 60  # 60秒
start_time = time.time()
# 进入循环，直到达到指定的时长
while time.time() - start_time <= duration:
    cur.execute('INSERT INTO pt_tb VALUES(1,1,1)')
conn.commit()  # 提交执行
cur.execute("SELECT COUNT(*) FROM pt_tb")
total_rows = cur.fetchone()[0]
print(f"数据库直接写总行数:{total_rows},每秒写{total_rows/60}行")
cur.execute("DELETE FROM pt_tb")
conn.commit()  # 提交执行


# 2.数据库中间件写性能
test_node = RaftSyncedSQLiteDB('localhost:4321', ['localhost:4322', 'localhost:4323'], 'performance_test')  # 连接性能测试数据库
time.sleep(2)
duration = 60
start_time = time.time()
# 进入循环，直到达到指定的时长
while time.time() - start_time <= duration:
    test_node.insert('pt_tb', [2, 2, 2])
test_node.close()  # 关闭本节点与数据库的连接
conn = sqlite3.connect(r'.\DB\syncDB\performance_test.db')
cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM pt_tb")
total_rows = cur.fetchone()[0]
print(f"数据库中间件写总行数:{total_rows},每秒写{total_rows/60}行")
cur.execute("DELETE FROM pt_tb")
conn.commit()  # 提交执行