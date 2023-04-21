import sqlite3
import time
import logging
import pprint
from pysyncobj import SyncObj, replicated
from Utils.utils import in_out_log
import collections  # 用于实现队列数据结构


# logging默认的日志级别是WARNING 只有级别大于或等于WARNING的日志消息才会被处理，低于该级别的消息将被忽略
logging.basicConfig(level=logging.INFO)  # 设置日志级别为INFO, 打印级别为INFO的日志消息

class SyncedSqliteDatabase(SyncObj):
    ''' Raft协议同步SQLite节点数据库

    Attribute:
        conn:当前节点连接的SQLite数据库
        __db:db_name
        __log_entry_queue:日志队列 记录节点操作日志，用于同步给其他节点
    '''
    def __init__(self, selfNodeAddr, partnerNodeAddrs, db_name):
        ''' 初始化基于Raft共识算法的sqlite节点

        Args:
            selfNodeAddr:节点套接字Socket=ip:
            partnerNodeAddrs:其他节点Socket
            db_name:该节点上的数据库名
        '''
        super(SyncedSqliteDatabase, self).__init__(selfNodeAddr, partnerNodeAddrs)  # pysyncobj初始化节点
        self.conn = sqlite3.connect(r'..\DB\syncDB\{}.db'.format(db_name))  # 创建，连接sqlite数据库
        self.__db = db_name  # 本节点数据库
        self.__log_entry_queue = collections.deque()  # 初始化日志队列，用双端队列实现日志条目队列，用于同步给其他节点

    # @replicated  # 只要调用 add_log_entry，就会同步更新其他节点的__log_entry_queue
    def add_log_entry(self, sql):
        ''' __log_entry_queue队尾添加sql操作语句

        记录某节点操作日志，用于同步给其他节点
        注：日志条目只收集sql语句
        :param sql: sql操作语句 DDL,DML,DCL str类型
        '''
        self.__log_entry_queue.append(sql)

    def get_log_entry(self):
        return self.__log_entry_queue
    def show_log_entry(self):
        print(self.__log_entry_queue)

    @in_out_log
    def create_table(self, table_name, table_info):
        ''' 在当前对象节点self创建一个表

        :param table_name:表名
        :param table_info:创建表的字段名，数据类型，是否可为NULL dict = {'column':column_name_list, 'datatype':datatype_list, 'isnull':isNULL_list}
        '''

        sql_create_table = f'CREATE TABLE {table_name}(\n'
        for i in range(len(table_info['column'])):
            if i == len(table_info['column']) -1 :  # sql语法最后一行不加 ,
                sql_create_table += table_info['column'][i] + ' ' \
                                    + table_info['datatype'][i] + '' \
                                    + table_info['isnull'][i] + '\n'
                break
            sql_create_table += table_info['column'][i]+' '+table_info['datatype'][i]+''+table_info['isnull'][i]+',\n'
        sql_create_table += r');'
        cursor = self.conn.cursor()  # 创建游标
        cursor.execute(sql_create_table)  # 执行语句
        self.conn.commit()  # 提交执行
        cursor.close()  # 关闭游标
        self.add_log_entry(sql_create_table)  # 操作日志入队
        print(sql_create_table)

    @in_out_log
    def insert(self, table_name, data):
        ''' 向table_name表中插入一条数据

        :param table_name: 表名
        :param data:数据 list
        '''
        sql_insert = f"INSERT INTO {table_name} VALUES("
        for d in data:
            sql_insert += '\'' + str(d) + '\'' + ','
        sql_insert = sql_insert[:-1]  # 删除最后不需要的 ','
        sql_insert += ')'
        cursor = self.conn.cursor()  # 创建游标
        cursor.execute(sql_insert)  # 执行语句
        self.conn.commit()  # 提交执行
        cursor.close()  # 关闭游标
        self.add_log_entry(sql_insert)  # 操作日志入队
        print(sql_insert)

    @in_out_log
    def insert_many(self, table_name, data):
        ''' 向table_name表中插入多条数据data

        :param table_name:表名
        :param data:要插入的数据 list of tuple [(),(),...]
        '''
        sql_insert_many = f'INSERT INTO {table_name} VALUES ('
        for _ in range(self.get_column_len(table_name)):  # 表有几列，在insert_many时就要有几个?
            sql_insert_many += '?,'
        sql_insert_many = sql_insert_many[:-1]  # 删除最后不需要的 ','
        sql_insert_many += ')'
        cursor = self.conn.cursor()  # 创建游标
        cursor.executemany(sql_insert_many, data)  # 执行语句
        self.conn.commit()  # 提交执行
        cursor.close()  # 关闭游标
        self.add_log_entry(sql_insert_many)  # 操作日志入队
        print(sql_insert_many)

    @in_out_log
    def get_column_info(self, table_name):
        ''' 获取table的所有列的详细信息list of tuple [(),(),...]

        cur.description属性获取查询结果的元数据，它返回一个元组列表，每个元组包含每个列的名称、类型、大小等信息
        :param table_name:要获取列名的表
        '''
        cur = self.conn.cursor()  # 创建游标
        sql_select = f'SELECT * FROM {table_name}'
        cur.execute(sql_select)
        self.add_log_entry(sql_select)
        pprint.pprint(cur.description)
        return cur.description

    @in_out_log
    def get_column_list(self, table_name):
        ''' 获取table所有列名的list

        :param table_name:表名
        '''
        desc = self.get_column_info(table_name)
        column_name_list = [column_info_tuple[0] for column_info_tuple in desc]
        print('colum_name_list:',column_name_list)
        return column_name_list

    @in_out_log
    def get_column_len(self, table_name):
        ''' 获取table所有列名的总长度

        '''
        print(len(self.get_column_list(table_name)))
        return len(self.get_column_list(table_name))

    @in_out_log
    def drop_table(self, table_name):
        ''' drop table by table_name

        :param table_name:表名
        '''
        sql_drop_table = f'DROP TABLE {table_name}'
        cur = self.conn.cursor()
        cur.execute(sql_drop_table)
        self.conn.commit()
        cur.close()
        self.add_log_entry(sql_drop_table)
        print(sql_drop_table)

    @in_out_log
    def drop_table_all(self):
        ''' 删除此节点db所有表

        每一个 SQLite 数据库都有一个叫 sqlite_master 的表，该表会自动创建
        sqlite_master是一个特殊表, 存储数据库的元信息, 如表(table), 索引(index), 视图(view), 触发器(trigger), 可通过select查询相关信息
        '''
        print(f'in {self.__db}.db')
        cur = self.conn.cursor()  # 创建游标
        # 查询数据库中的所有表名
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        table_names = cur.fetchall()  # 获取查询结果集中的所有行 list of tuple [('node1_tb1',), ('node1_tb2',)]
        for table_name in table_names:  # 对该node db的table逐一drop
            sql_drop_table = f'DROP TABLE {table_name[0]}'
            self.add_log_entry(sql_drop_table)
            print(sql_drop_table)
            cur.execute(sql_drop_table)
        self.conn.commit()  # 提交操作
        cur.close()  # 关闭游标

    def get_db_name(self):
        return self.__db

    def close(self):  # 关闭本节点与数据库的连接
        self.conn.close()

    def get(self, key):
        cursor = self.conn.cursor()
        cursor.execute("SELECT value FROM mytable WHERE key = ?", (key,))
        row = cursor.fetchone()
        if row is not None:
            return row[0]
        else:
            return None

def create_table(table_name, table_info):
    sql_create_table = f'''CREATE TABLE {table_name}(
    '''
    for i in range(len(table_info['column'])):
        if i == len(table_info['column']) - 1:  # sql语法最后一行不加 ,
            sql_create_table += table_info['column'][i] + ' ' \
                                + table_info['datatype'][i] + '' \
                                + table_info['isnull'][i] + '\n'
            break
        sql_create_table += table_info['column'][i] + ' ' + table_info['datatype'][i] + '' + table_info['isnull'][
            i] + ',\n'
    sql_create_table += r');'
    print(sql_create_table)

def insert(table_name, data):
    ''' 向table_name表中插入一条数据

    :param table_name: 表名
    :param data:数据 list
    '''
    sql_insert = f"INSERT INTO {table_name} VALUES("
    for d in data:
        sql_insert += '\'' + str(d) + '\'' + ','
    sql_insert = sql_insert[:-1]  # 删除最后不需要的 ','
    sql_insert += ')'
    print(sql_insert)

def insert_many(column_len, table_name, data):
    sql_insert_many = f'INSERT INTO {table_name} VALUES ('
    for _ in range(column_len):  # 表有几列，在insert_many时就要有几个?
        sql_insert_many += '?,'
    sql_insert_many = sql_insert_many[:-1]  # 删除最后不需要的 ','
    sql_insert_many += ')'
    print(sql_insert_many)

# KV demo

if __name__ == '__main__':
    column_name_list = ['column1', 'column2', 'column3']
    datatype_list = ['TEXT', 'INT', 'CHAR(50)']
    isNULL_list = ['NOT NULL', 'NOT NULL', 'NOT NULL']
    table_info = {'column':column_name_list, 'datatype':datatype_list, 'isnull':isNULL_list}
    # For test 测试用
    # create_table('tb1', table_info)
    # insert('tb1', ['v1', 'v2', 'v3', 1, 2, 3])
    # # insert_many(6,'tb1',[])
    # breakpoint()
    node1 = SyncedSqliteDatabase('localhost:4321', ['localhost:4322', 'localhost:4323'], 'test1')  # test1.db
    node2 = SyncedSqliteDatabase('localhost:4322', ['localhost:4321', 'localhost:4323'], 'test2')  # test2.db
    node3 = SyncedSqliteDatabase('localhost:4323', ['localhost:4321', 'localhost:4322'], 'test3')  # test3.db

    time.sleep(5)
    pprint.pprint(node1.getStatus())  # 格式化输出
    pprint.pprint(node2.getStatus())  # 格式化输出
    pprint.pprint(node3.getStatus())  # 格式化输出
    node1.drop_table_all()
    node2.drop_table_all()
    node3.drop_table_all()

    node1.create_table('node1_tb1', table_info)  # 创建表，在DBeaver中查看
    node1.create_table('node1_tb2', table_info)  # 创建表，在DBeaver中查看
    node2.create_table('node2_tb1', table_info)  # 创建表，在DBeaver中查看
    node3.create_table('node3_tb1', table_info)  # 创建表，在DBeaver中查看

    # 展示某时间点某Node的__log_entry_queue
    node1.show_log_entry()
    node2.show_log_entry()
    node3.show_log_entry()

