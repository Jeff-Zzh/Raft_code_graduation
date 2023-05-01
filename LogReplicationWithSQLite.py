import sqlite3
import time
import logging
import pprint
from pysyncobj import SyncObj, replicated
from Utils.utils import in_out_log, slogan_path, myprint
import collections  # 用于实现队列数据结构
from pywebio.input import *
from pywebio.output import *

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
        self.conn = sqlite3.connect(r'.\DB\syncDB\{}.db'.format(db_name))  # 创建(DB不存在)\连接(DB存在)sqlite数据库
        self.__db = db_name  # 本节点数据库
        self.__log_entry_queue = collections.deque()  # 初始化日志队列，用双端队列实现日志条目队列，用于同步给其他节点
        self.role = self.get_role()  # 初始都为follower，需要用refresh_role()方法刷新当前节点角色

    @replicated  # 只要调用 add_log_entry，就会同步更新其他节点的__log_entry_queue
    def add_log_entry(self, sql):
        ''' __log_entry_queue队尾添加sql操作语句

        记录某节点操作日志，用于同步给其他节点
        注：日志条目只收集sql语句
        :param sql: sql操作语句 DDL,DML,DCL str类型
        '''
        self.__log_entry_queue.append(sql)

    @replicated  # 无效果，已废弃
    def replicated_log_entry(self):
        ''' 复制操作日志至其他节点

        '''
        self.__log_entry_queue = self.get_log_entry()

    def replicated_log_entry_manual(self, other_node):
        ''' 复制日志至其他节点

        :param other_node:其他节点 [node1,node2,...]
        '''
        for node in other_node:
            node.set_log_entry(self.__log_entry_queue)

    def set_log_entry(self, other_log_entry_queue):
        self.__log_entry_queue = other_log_entry_queue

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
    def drop_table_all(self, whether_add = True):
        ''' 删除此节点db所有表

        每一个 SQLite 数据库都有一个叫 sqlite_master 的表，该表会自动创建
        sqlite_master是一个特殊表, 存储数据库的元信息, 如表(table), 索引(index), 视图(view), 触发器(trigger), 可通过select查询相关信息
        :param whether_add:是否将此sql添加到本届点的日志，并同步给其他节点，默认为True
        '''
        print(f'in {self.__db}.db')
        cur = self.conn.cursor()  # 创建游标
        # 查询数据库中的所有表名
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        table_names = cur.fetchall()  # 获取查询结果集中的所有行 list of tuple [('node1_tb1',), ('node1_tb2',)]
        for table_name in table_names:  # 对该node db的table逐一drop
            sql_drop_table = f'DROP TABLE {table_name[0]}'
            if whether_add == True:
                self.add_log_entry(sql_drop_table)
            print(sql_drop_table)
            cur.execute(sql_drop_table)
        self.conn.commit()  # 提交操作
        cur.close()  # 关闭游标

    def get_all_tb(self):
        ''' 获取当前节点的SQLite数据库的所有表，list of tuple形式

        每一个 SQLite 数据库都有一个叫 sqlite_master 的表，该表会自动创建
        sqlite_master是一个特殊表, 存储数据库的元信息, 如表(table), 索引(index), 视图(view), 触发器(trigger), 可通过select查询相关信息
        '''
        cur = self.conn.cursor()  # 创建游标
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return cur.fetchall()

    def get_db_name(self):
        return self.__db

    def close(self):  # 关闭本节点与数据库的连接
        self.conn.close()

    def refresh_role(self):
        ''' 刷新当前节点角色，初始都为follower

        '''
        role_dic = {0: 'follower', 1: 'candidate', 2: 'leader'}
        self.role = self.getStatus()['state']

    def get_role(self):
        ''' Raft协议中，本届点角色

        '''
        role_dic = {0:'follower', 1:'candidate', 2:'leader'}
        return role_dic[self.getStatus()['state']]

    def get_self(self):
        ''' 获取本节点Socket 如TCPNode('localhost:4323')

        '''
        return self.getStatus()['self']

def create_table(table_name, table_info):
    sql_create_table = f'''CREATE TABLE {table_name}(
    '''
    for i in range(len(table_info['column'])):
        if i == len(table_info['column']) - 1:  # sql语法最后一行不加 ,
            sql_create_table += table_info['column'][i] + ' ' \
                                + table_info['datatype'][i] + '' \
                                + table_info['isnull'][i] + '\n'
            break
        sql_create_table += table_info['column'][i] + ' ' + table_info['datatype'][i] + '' + table_info['isnull'][i] + ',\n'
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

def build_raft_cluster(node_to_be_buiild):
    ''' 初始化/构建 遵守raft协议的集群

    node格式：键值对 {'localhost:4321':'test1'}
    :param node_to_be_buiild:要初始化的集群list of dic [{'localhost:4321':'test1}]
    Returns:
        node_list:构建好的基于Raft协议的SQLite数据库节点集群
    '''
    node_list = []  # 构建好的node存放的list
    for node_index in range(len(node_to_be_buiild)):
        other_node_list = node_to_be_buiild[:node_index] + node_to_be_buiild[node_index+1:]  # 其他节点list
        other_node_address = []  # 其他节点地址list
        for other_node in other_node_list:
            other_node_address.append(list(other_node.keys())[0])
        node = SyncedSqliteDatabase(list(node_to_be_buiild[node_index].keys())[0],
                                    other_node_address,
                                    list(node_to_be_buiild[node_index].values())[0] )  # list()[0]把dict_keys类型转为str
        node_list.append(node)
    return node_list

def init_web():
    ''' 初始化web界面，展示对应DIY界面

    '''
    img = open(file=slogan_path, mode='rb').read()  # 二进制读图片文件，put_image
    put_image(img)
    put_markdown('# Log Replication Demo With SQLite')

def init_table_info():
    ''' 初始化表格信息，并在web端展示

    '''
    # table info
    myprint('show table info')
    column_name_list = ['column1', 'column2', 'column3']
    datatype_list = ['TEXT', 'INT', 'CHAR(50)']
    isNULL_list = ['NOT NULL', 'NOT NULL', 'NOT NULL']
    table_info = {'column': column_name_list, 'datatype': datatype_list, 'isnull': isNULL_list}
    put_table([column_name_list,
               datatype_list,
               isNULL_list], header=[span('table info', col=3)])
    # For test 测试用，打印到命令行
    # create_table('tb1', table_info)
    # insert('tb1', ['v1', 'v2', 'v3', 1, 2, 3])
    # # insert_many(6,'tb1',[])
    # breakpoint()
    return table_info

if __name__ == '__main__':
    init_web()
    # Client与主节点leader进行交互，leader与其他Nodes(followers)由Raft保持关系
    # 构建基于Raft协议的SQLite数据库节点集群
    node_to_be_build = []  # 将要被构建为Raft集群的节点list  [{'localhost:4321':'test1'}, {'localhost:4322':'test2'}, ...]
    node_num = input('How many nodes in Raft-SQLite cluster?', type=TEXT, placeholder='请输入数字，如3', required=True)
    for i in range(int(node_num)):  # eval(node_num)
        info = input_group(f'Raft Node {i+1}',[
            input('Input Node Socket address', name='socket'),
            input('Input Node DB name', name='DB')
        ])
        node_to_be_build.append({info['socket']:info['DB']})

    node_list = build_raft_cluster(node_to_be_buiild=node_to_be_build)

    time.sleep(5)
    # 在web展示构建好的节点及其状态
    myprint('⏰Building nodes, please wait........')
    myprint('Raft节点集群')

    node_self_DB_list = []  # list of list [[TCPNode('localhost:4321'), 'test1'], ...]
    for node in node_list:
        pprint.pprint(node.getStatus())  # 格式化输出
        node_self_DB = []  # 存储TCPNode(Socket)节点，存储节点DB名
        node_self_DB.append(node.get_self())  # self
        node_self_DB.append(node.get_db_name())  # DB name
        node_self_DB_list.append(node_self_DB)
    put_table(node_self_DB_list, header=['Node', 'DB'])

    # 检测如果任意sqlite节点有表->删除->建立干净的实验环境
    for node in node_list:
        if len(node.get_all_tb()):
            print('删除node{}的所有表'.format(node.getStatus()['self']))
            node.drop_table_all(whether_add=False)  # 不添加到节点日志中

    # 只操作表1-表1是leader-raftAppendEntry同步操作日志给follower
    # 表1是follower-找到leader-让Client直接和leader交互-leader同步操作日志给follower
    table_info = init_table_info()
    node1.create_table('node1_tb1', table_info)  # 创建表，在DBeaver中查看
    node1.create_table('node1_tb2', table_info)  # 创建表，在DBeaver中查看
    # node2.create_table('node2_tb1', table_info)  # 创建表，在DBeaver中查看
    # node3.create_table('node3_tb1', table_info)  # 创建表，在DBeaver中查看


    time.sleep(3)  # 等待日志复制 @replicated函数修饰器是异步调用的 Function will be called asynchronously
    # 展示某时间点某Node的__log_entry_queue
    print('*' * 10, '日志复制 前 各节点日志队列', '*' * 10)
    node1.show_log_entry()
    node2.show_log_entry()
    node3.show_log_entry()

    # node1.replicated_log_entry()  # 进行日志复制
    node1.replicated_log_entry_manual([node2, node3])  # 进行日志复制
    time.sleep(3)
    print('*' * 10, '日志复制 后 各节点日志队列', '*' * 10)
    node1.show_log_entry()
    node2.show_log_entry()
    node3.show_log_entry()

