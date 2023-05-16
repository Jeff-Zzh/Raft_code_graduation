import time
import os
import sqlite3
from Utils.RaftSyncedSQLiteDB import RaftSyncedSQLiteDB
os.chdir('E:\Raft')  # 工作路径切换


# 创建测试用节点和performance_test.db
test_node = RaftSyncedSQLiteDB('localhost:4321', ['localhost:4322', 'localhost:4323'], 'performance_test')
time.sleep(2)

# 创建性能测试用表pt_tb 程序运行前确保系统没有该表
column_name_list = ['column1', 'column2', 'column3']
datatype_list = ['TEXT', 'INT', 'CHAR(50)']
isNULL_list = ['NOT NULL', 'NOT NULL', 'NOT NULL']
table_info = {'column': column_name_list, 'datatype': datatype_list, 'isnull': isNULL_list}
test_node.create_table('pt_tb', table_info)
test_node.close()  # 关闭本节点与数据库的连接

conn = sqlite3.connect(r'.\DB\syncDB\performance_test.db')
cur = conn.cursor()

# 插入数据 10 秒
duration = 10
start_time = time.time()
# 进入循环，直到达到指定的时长
while time.time() - start_time <= duration:
    cur.execute('INSERT INTO pt_tb VALUES(1,1,1)')
conn.commit()  # 提交执行
cur.execute("SELECT COUNT(*) FROM pt_tb")
total_rows = cur.fetchone()[0]

# 1.数据库直接读数据
# 记录开始时间
start_time = time.time()

# 多次运行查询并计算平均时间
num_runs = 10
total_time = 0
for i in range(num_runs):  # 读取表中所有内容，重复10次
    # 执行查询
    cur.execute('SELECT * FROM pt_tb')
    # 获取所有结果
    results = cur.fetchall()
    # 记录时间
    end_time = time.time()
    total_time += (end_time - start_time)
    # 重置开始时间
    start_time = end_time

# 计算读取表中所有内容1次的平均时间
avg_time = total_time / num_runs

# 输出平均查询时间
print(f"数据库直接读取表中 {total_rows} 行所有内容1次的平均查询时间（秒）: {avg_time}")

# 2.数据库复制中间件读数据
# 创建测试用节点和performance_test.db
test_node = RaftSyncedSQLiteDB('localhost:4321', ['localhost:4322', 'localhost:4323'], 'performance_test')
time.sleep(2)
# 记录开始时间
start_time = time.time()

# 多次运行查询并计算平均时间
num_runs = 10
total_time = 0
for i in range(num_runs):  # 读取表中所有内容，重复10次
    # 执行查询 & 获取所有结果
    results = test_node.select('pt_tb', '*')
    # 记录时间
    end_time = time.time()
    total_time += (end_time - start_time)
    # 重置开始时间
    start_time = end_time

# 计算读取表中所有内容1次的平均时间
avg_time = total_time / num_runs

# 输出平均查询时间
print(f"数据库复制中间件读取表中 {total_rows} 行所有内容1次的平均查询时间（秒）: {avg_time}")