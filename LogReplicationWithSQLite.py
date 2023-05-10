import sqlite3
import time
import os
import logging  # 用于命令行格式化日志输出
import pprint  # 用于命令行格式化打印
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
        if not os.path.exists('.\DB\syncDB'):  # 创建节点DB存储文件夹
            os.mkdir('.\DB\syncDB')
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

    def replicated_log_entry_manual(self, other_node):
        ''' 复制日志至其他节点

        :param other_node:其他节点 [node1,node2,...] list of SyncedSqliteDatabase类的实例化对象
        '''
        for node in other_node:
            node.set_log_entry(self.__log_entry_queue)

    @replicated  # 无效果，已废弃
    def replicated_log_entry(self):
        ''' 复制操作日志至其他节点

        '''
        self.__log_entry_queue = self.get_log_entry()


    def set_log_entry(self, other_log_entry_queue):
        self.__log_entry_queue = other_log_entry_queue

    def get_log_entry(self):
        return self.__log_entry_queue

    def show_log_entry(self):
        myprint('in ' + str(self.get_self()))  # 打印在哪个节点中
        put_text(self.__log_entry_queue)
        print(self.__log_entry_queue)


    # DDL(Data Definition Language) 数据定义语言
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

    def check_table_exist(self, target_table_name):
        ''' 检查table在当前node数据库中是否存在

        每一个 SQLite 数据库都有一个叫 sqlite_master 的表，该表会自动创建
        sqlite_master是一个特殊表, 存储数据库的元信息, 如表(table), 索引(index), 视图(view), 触发器(trigger), 可通过select查询相关信息
        :param table_name:查询是否存在的表
        '''
        print(f'in {self.__db}.db')
        cur = self.conn.cursor()  # 创建游标
        # 查询数据库中的所有表名
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        table_names = cur.fetchall()  # 获取查询结果集中的所有行 list of tuple [('node1_tb1',), ('node1_tb2',)]
        for table_name in table_names:  # 检擦该node db的table有无目标table
            if table_name[0] == target_table_name:
                return True
        return False  # 查完了都没有，那就是没有target_table

    @in_out_log
    def alter_table_name(self, old_table_name, new_table_name):
        ''' 重命名一个已经存在的表

        :param old_table_name:要重命名的表
        :param new_table_name:新表名
        '''
        if not self.check_table_exist(target_table_name=old_table_name):
            myprint('要重命名的表名不存在，检查输入是否正确 或 用get_all_tb()检查db是否有该表')
            return
        sql_alter_table_rename = f'ALTER TABLE {old_table_name} RENAME TO {new_table_name};'
        cursor = self.conn.cursor()  # 创建游标
        cursor.execute(sql_alter_table_rename)  # 执行语句
        self.conn.commit()  # 提交执行
        cursor.close()  # 关闭游标
        self.add_log_entry(sql_alter_table_rename)  # 操作日志入队
        print(sql_alter_table_rename)

    @in_out_log
    def alter_add_column(self, table_name, column_name, data_type, canNULL):
        ''' 向表中添加新的列

        Args:
            table_name:要添加列的表的名称
            column_name:要添加的列的名
            data_type:要添加的列的数据类型
            canNULL:列是否可为空的约束条件 True | False
        '''
        if not self.check_table_exist(target_table_name=table_name):
            myprint('要添加列的表名不存在，检查输入是否正确 或 用get_all_tb()检查db是否有该表')
            return
        # 向表中添加一个定义为“NOT NULL”的列时，必须为该列提供一个默认值，以便在插入新行时自动填充该列
        canNULL = 'NULL' if canNULL else 'NOT NULL DEFAULT \'\''  # 新列的值可以为NULL或不能为NULL，不能为NULL需要提供默认值
        sql_alter_table_add_column = f'ALTER TABLE {table_name} ADD COLUMN {column_name} {data_type} {canNULL};'
        cursor = self.conn.cursor()  # 创建游标
        cursor.execute(sql_alter_table_add_column)  # 执行语句
        self.conn.commit()  # 提交执行
        cursor.close()  # 关闭游标
        self.add_log_entry(sql_alter_table_add_column)  # 操作日志入队
        print(sql_alter_table_add_column)

    @in_out_log
    def alter_drop_column_method1(self, table_name, column_name):
        ''' 删除表中的列，SQLite3.35.0以上的版本才能用alter table drop column,python 3.7.8内置sqlite3.31.1
        原想法3.31.1->3.35.0 upgrade->失败，pip报错，需要C++版本环境

        :param table_name:要删除列的表的名称
        :param column_name:要删除的列的名称
        '''
        if not self.check_table_exist(target_table_name=table_name):
            myprint('要删除列的表名不存在，检查输入是否正确 或 用get_all_tb()检查db是否有该表')
            return
        sql_alter_table_drop_column = f'ALTER TABLE {table_name} DROP COLUMN {column_name};'
        cursor = self.conn.cursor()  # 创建游标
        cursor.execute(sql_alter_table_drop_column)  # 执行语句
        self.conn.commit()  # 提交执行
        cursor.close()  # 关闭游标
        self.add_log_entry(sql_alter_table_drop_column)  # 操作日志入队
        print(sql_alter_table_drop_column)

    @in_out_log
    def alter_drop_column_method2(self, table_name, column_name):
        ''' 删除表中的列 Python内置的SQLite3 3.31.1版本不支持ALTER TABLE语句中的DROP COLUMN操作

        1.PRAGMA table_info({}) 获取表中的所有列名  PRAGMA是SQLite中的一个特殊命令，用于查询和设置一些特定的数据库状态和元数据
        PRAGMA table_info命令可以用于查询指定表的列信息，包括列名、数据类型、是否为主键等。该命令返回一个包含列信息的结果集，其中每一行包含有关某个列的信息
        2.创建一个新的表，包含要删除的列之外的所有列
        3.从旧表中选择除了要删除列的其他列的所有行，将它们插入到新表中
        4.删除旧表
        5.将新表重命名为’旧表‘的名称
        6.记录log_entry 提交更改 关闭游标

        :param table_name:要删除列的表的名称
        :param column_name:要删除的列的名称
        '''
        if not self.check_table_exist(target_table_name=table_name):
            myprint('要删除列的表名不存在，检查输入是否正确 或 用get_all_tb()检查db是否有该表')
            return
        # 获取表中的所有列名
        sql_get_all_table_info = 'PRAGMA table_info({})'.format(table_name)
        cursor = self.conn.execute(sql_get_all_table_info)
        columns = [row[1] for row in cursor.fetchall()]
        self.add_log_entry(sql_get_all_table_info)

        # 创建一个新的表，包含要删除的列之外的所有列
        new_columns = [col for col in columns if col != column_name]  # 新表的所有列，不包含要删除的那一列
        new_table_name = 'new_' + table_name
        sql_create_new_table = 'CREATE TABLE {} ({})'.format(new_table_name,
                                                   ', '.join(['{} TEXT'.format(col) for col in new_columns]))
        # ', '.join(...)是一个字符串方法，用于将字符串列表中的所有元素连接为一个字符串，其中每个元素之间用 逗号 和 空格 分隔。
        # 例如，如果字符串列表是['col1 TEXT', 'col2 TEXT', 'col3 TEXT']，则该表达式将生成'col1 TEXT, col2 TEXT, col3 TEXT'。
        # ['{} TEXT'.format(col) for col in new_columns]是一个列表推导式，将列名列表new_columns中的每个列名插入到格式化字符串'{} TEXT'中，
        # 并生成一个新的字符串列表。例如，如果new_columns是['col1', 'col2', 'col3']，则该表达式将生成['col1 TEXT', 'col2 TEXT', 'col3 TEXT']。
        self.conn.execute(sql_create_new_table)
        self.add_log_entry(sql_create_new_table)

        # 从旧表中选择除了要删除列的其他列的所有行，将它们插入到新表中
        sql_select = 'SELECT {} FROM {}'.format(', '.join(new_columns), table_name)
        sql_insert = 'INSERT INTO {} ({}) {}'.format(new_table_name, ', '.join(new_columns), sql_select)
        # 'INSERT INTO new_table (col1, col2, col3) SELECT col1, col2, col3 FROM old_table'
        self.conn.execute(sql_insert)
        self.add_log_entry(sql_select)

        # 删除旧表
        sql_drop_old_table = 'DROP TABLE {}'.format(table_name)
        self.conn.execute(sql_drop_old_table)
        self.add_log_entry(sql_drop_old_table)

        # 将新表重命名为’旧表‘的名称
        sql_rename_new_table_to_old_table = 'ALTER TABLE {} RENAME TO {}'.format(new_table_name, table_name)
        self.conn.execute(sql_rename_new_table_to_old_table)
        self.add_log_entry(sql_rename_new_table_to_old_table)

        self.conn.commit()  # 提交更改

    @in_out_log
    def get_column_info(self, table_name):
        ''' 获取某table的所有列的详细信息list of tuple [(),(),...]

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


    # DML(Data Manipulation Language)  数据操作语言
    @in_out_log
    def insert(self, table_name, data):
        ''' 向table_name表中插入一条数据

        :param table_name: 表名
        :param data:数据 list
        '''
        if not self.check_table_exist(target_table_name=table_name):
            myprint('要insert行的表名不存在，检查输入是否正确 或 用get_all_tb()检查db是否有该表')
            return
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
        if not self.check_table_exist(target_table_name=table_name):
            myprint('要insert行的表名不存在，检查输入是否正确 或 用get_all_tb()检查db是否有该表')
            return
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

    def update(self, table_name, set_column, value, where=False, condition=None):
        ''' update：修改表中已有行的某一列的值
        如果where=True, condition传入选定要修改的行的条件，否则所有的行都会被更新

        Args:
            table_name:要update的表名
            set_column:要SET值的列
            value:要给set_column列的值
            where:是否要筛选行
            condition:筛选条件 如 'ID = 6'  ' col1 = 'some_value' ' 值是str在sql中要加单引号
        '''
        if not self.check_table_exist(target_table_name=table_name):
            myprint('要update的表名不存在，检查输入是否正确 或 用get_all_tb()检查db是否有该表')
            return
        value = '\'' + value + '\'' # sql语句中需要加单引号
        sql_update = ''
        if where:
            sql_update = f'UPDATE {table_name} SET {set_column} = {value} WHERE {condition};'
        else:
            sql_update = f'UPDATE {table_name} SET {set_column} = {value};'
        cursor = self.conn.cursor()  # 创建游标
        cursor.execute(sql_update)  # 执行语句
        self.conn.commit()  # 提交执行
        cursor.close()  # 关闭游标
        self.add_log_entry(sql_update)  # 操作日志入队
        print(sql_update)

    def delete(self, table_name, where=False, condition=None):
        ''' 删除某表已有的记录 用带有 WHERE 子句的 DELETE 查询来删除选定行，否则所有的记录都会被删除

         Args:
            table_name:要delete的表名
            where:是否要筛选行
            condition:筛选条件 如 'ID = 6'  ' col1 = 'some_value' ' 值是str在sql中要加单引号
        '''
        if not self.check_table_exist(target_table_name=table_name):
            myprint('要delete行的表名不存在，检查输入是否正确 或 用get_all_tb()检查db是否有该表')
            return
        sql_delete = ''
        if where:
            sql_delete = f'DELETE FROM {table_name} WHERE {condition}'
        else:
            sql_delete = f'DELETE FROM {table_name}'
        cursor = self.conn.cursor()  # 创建游标
        cursor.execute(sql_delete)  # 执行语句
        self.conn.commit()  # 提交执行
        cursor.close()  # 关闭游标
        self.add_log_entry(sql_delete)  # 操作日志入队
        print(sql_delete)

    def select(self, table_name, column_list):
        ''' SELECT 语句用于从 SQLite 数据库表中获取数据，以结果表的形式返回数据

        :param table_name:要select的表名
        :param column_list:从表中要筛选的列名 list of str 或者 为*，即从表中筛选全部的行
        Returns:
            rows:表中被select出的行
        '''
        if not self.check_table_exist(target_table_name=table_name):
            myprint('要delete行的表名不存在，检查输入是否正确 或 用get_all_tb()检查db是否有该表')
            return
        sql_select = ''
        if column_list == '*':
            sql_select = f'SELECT * FROM {table_name};'
        else:
            columns = ','.join(column_list)
            sql_select = f'SELECT {columns} FROM {table_name};'
        cursor = self.conn.cursor()  # 创建游标
        cursor.execute(sql_select)  # 执行语句
        rows = cursor.fetchall()
        cursor.close()  # 关闭游标
        self.add_log_entry(sql_select)  # 操作日志入队
        myprint(sql_select)
        myprint(rows)
        return rows

    # DCL(Data Control Language) 数据控制语言
    def grant(self, permission, object, user):
        ''' 授予用户或用户组对数据库对象的权限

        Args:
            permission:要授予的权限类型 SELECT/DELETE/UPDATE/INSERT
            object:要授予权限的对象，可以是表、视图或其他数据库对象的名称
            user:要授予权限的用户或用户组的名称
        '''
        sql_grant = f'GRANT {permission} ON {object} TO {user}'
        cursor = self.conn.cursor()  # 创建游标
        cursor.execute(sql_grant)  # 执行语句
        self.conn.commit()  # 提交执行
        cursor.close()  # 关闭游标
        self.add_log_entry(sql_grant)  # 操作日志入队
        print(sql_grant)

    def revoke(self, permission, object, user):
        ''' 授予用户或用户组对数据库对象的权限

        Args:
            permission:撤销的权限类型 SELECT/DELETE/UPDATE/INSERT
            object:要撤销权限的对象，可以是表、视图或其他数据库对象的名称
            user:要撤销权限的用户或用户组的名称
        '''
        sql_revoke = f'REVOKE {permission} ON {object} FROM {user}'
        cursor = self.conn.cursor()  # 创建游标
        cursor.execute(sql_revoke)  # 执行语句
        self.conn.commit()  # 提交执行
        cursor.close()  # 关闭游标
        self.add_log_entry(sql_revoke)  # 操作日志入队
        print(sql_revoke)

    def get_table_info(self, table_name):
        ''' 返回table_name表的表信息

        查询名为table_name的表的信息。查询语句中，sqlite_master是SQLite3系统表，包含了所有表的信息。我们筛选出其中type为'table'，
        name为'table_name'的表。接下来，我们使用fetchone()方法获取查询结果的第一行，即table_name表的信息
        '''
        # 如果查询语句中包含字符串类型的参数，需要使用单引号将参数括起来，否则SQLite3会将参数解释为列名，从而导致no such column的错误
        table_name = '\'' + table_name + '\''  # 需要使用单引号括起来，才是表名
        cursor = self.conn.cursor()  # 创建游标
        cursor.execute(f"SELECT * FROM sqlite_master WHERE type='table' AND name={table_name}")
        # 获取查询结果
        result = cursor.fetchone()
        print(result)
        cursor.close()
        return result

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
        self.role = role_dic[self.getStatus()['state']]

    def get_role(self):
        ''' Raft协议中，本届点角色

        '''
        role_dic = {0:'follower', 1:'candidate', 2:'leader'}
        return role_dic[self.getStatus()['state']]

    def get_self(self):
        ''' 获取本节点Socket 如TCPNode('localhost:4323')

        '''
        return self.getStatus()['self']


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
    myprint('Show Table Info')
    column_name_list = ['column1', 'column2', 'column3']
    datatype_list = ['TEXT', 'INT', 'CHAR(50)']
    isNULL_list = ['NOT NULL', 'NOT NULL', 'NOT NULL']
    table_info = {'column': column_name_list, 'datatype': datatype_list, 'isnull': isNULL_list}
    put_table([column_name_list,
               datatype_list,
               isNULL_list], header=[span('Table Info', col=3)])

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
    # For test 测试用，打印到命令行
    # create_table('tb1', table_info)
    # insert('tb1', ['v1', 'v2', 'v3', 1, 2, 3])
    # # insert_many(6,'tb1',[])
    # breakpoint()

    return table_info

def user_choose_node(node_list):
    ''' 用户选择要操作的节点，web交互

    :param node_list:要操作的node节点列表
    '''
    node_self_list = []  # 存储每个Node的self
    for node in node_list:
        node_self_list.append(str(node.get_self()))
    node_socket = select('选择你要操作的SQLite节点', node_self_list)  # node_self_list必须是list of str
    return node_socket  # 返回用户选择的要操作的node_socket

def node_socket_mapping(node_list):
    ''' 返回dict，保存node和其socket的映射关系{ {'localhost:4321:node1'} }

    :param node_list:list of SyncedSqliteDatabase类都西昂
    Returns:
        Dict:node和其socket的映射关系{ {'localhost:4321:node1'} }
    '''
    node_socket_map = {}
    for node in node_list:
        node_socket_map[str(node.get_self())] = node
    return node_socket_map

def put_node_info(node):
    ''' 打印node节点信息 table,log_entry

    '''
    put_text('节点SQLite表：', node.get_all_tb())
    put_text('该节点日志条目Log Entry', node.get_log_entry())

def show_operation():
    put_markdown('## Raft-SQLite集群支持的web交互功能')
    put_table([
        [span('CREATE TABLE', row=2), 'INSERT', span('GRANT', row=2)],
        ['INSERT MANY'],
        ['ALTER TABLE RENAME TO', span('UPDATE', row=4), span('REVOKE', 4)],
        ['ALTER TABLE ADD COLUMN'],
        ['ALTER TABLE DROP COLUMN menthod1'],
        ['ALTER TABLE DROP COLUMN menthod2'],
        ['DROP TABLE', span('DELETE', row=2), span('--', row=3)],
        ['DROP TABLE ALL'],
        ['--', 'SELECT']
    ], header=['DDL', 'DML', 'DCL'])

def test():
    ''' 开发代码时，测试用函数

    '''
    column_name_list = ['column1', 'column2', 'column3']
    datatype_list = ['TEXT', 'INT', 'CHAR(50)']
    isNULL_list = ['NOT NULL', 'NOT NULL', 'NOT NULL']
    table_info = {'column': column_name_list, 'datatype': datatype_list, 'isnull': isNULL_list}
    node1 = SyncedSqliteDatabase('localhost:4321', ['localhost:4322', 'localhost:4323'], 'test1')  # test1.db
    node2 = SyncedSqliteDatabase('localhost:4322', ['localhost:4321', 'localhost:4323'], 'test2')  # test2.db
    node3 = SyncedSqliteDatabase('localhost:4323', ['localhost:4321', 'localhost:4322'], 'test3')  # test3.db
    node_list = [node1, node2, node3]
    node1.create_table('tb1',table_info)
    node1.alter_table_name('tb1', 'new_tb1')
    node1.alter_add_column('new_tb1', 'column_alter', 'VARCHAR(50)', canNULL=False)  # 不允许此列的值为空
    # node1.alter_drop_column_method1('new_tb1', 'column1')  版本原因，用不了
    node1.alter_drop_column_method2('new_tb1', 'column1')
    put_text(node1.get_table_info('new_tb1'))  # 获取表信息
    node1.insert('new_tb1',[1,1,1])  # 先插入一行，用于下面查看update的结果
    node1.update('new_tb1','column_alter', 'testing1')
    node1.insert('new_tb1', [2, 2, 2])  # 检查update的 WHERE condition功能
    node1.update('new_tb1', 'column_alter', 'testing2', where=True, condition='column2 = 2')
    node1.insert('new_tb1', ['3', '3', '3'])  # 检查update的 WHERE condition功能
    node1.update('new_tb1', 'column_alter', 'testing2', where=True, condition='column3 = \'3\'')
    node1.delete('new_tb1', where=True, condition='column3 = \'3\'')
    node1.select('new_tb1', '*')  # SELECT *
    node1.select('new_tb1', ['column3'])
    node1.select('new_tb1', ['column2', 'column3'])
    node1.delete('new_tb1')  # 删除new_tb1表所有行
    breakpoint()

if __name__ == '__main__':
    # TODO 性能测试 写/s 中间件加入前后 可用性测试
    # test()

    init_web()  # 初始化web界面
    # web客户端 用户控制 构建基于Raft协议的SQLite数据库节点集群
    node_to_be_build = []  # 将要被构建为Raft集群的节点list  [{'localhost:4321':'test1'}, {'localhost:4322':'test2'}, ...]
    node_num = input('How many nodes in Raft-SQLite cluster?', type=TEXT, placeholder='请输入数字，如3', required=True)
    for i in range(int(node_num)):  # eval(node_num)
        info = input_group(f'Raft Node {i+1}', [
            input('Input Node Socket address', name='socket'),
            input('Input Node DB name', name='DB')
        ])
        node_to_be_build.append({info['socket']:info['DB']})

    node_list = build_raft_cluster(node_to_be_buiild=node_to_be_build)  # Raft集群构建函数

    myprint('⏰Building nodes, please wait........')  # 等待构建
    loop_time = 5
    end_time = time.time() + loop_time
    while True:  # 循环loop_time
        myprint('Refreshing Node State')
        node_self_DB_Role_list = []  # list of list [[TCPNode('localhost:4321'), 'test1', 'leader'], ...]
        for node in node_list:
            pprint.pprint(node.getStatus())  # 命令行格式化输出
            node_self_DB_Role = []  # 存储TCPNode(Socket)节点，存储节点DB名
            node_self_DB_Role.append(node.get_self())  # self
            node_self_DB_Role.append(node.get_db_name())  # DB name
            node_self_DB_Role.append(node.get_role())  # Node Role
            node_self_DB_Role_list.append(node_self_DB_Role)
        put_table(node_self_DB_Role_list, header=['Node', 'DB', 'Role'])
        time.sleep(0.5)  # 打印间隔
        if time.time() > end_time:
            break

    # 在web展示构建好的节点及其状态
    put_markdown('## Raft节点集群')
    node_self_DB_Role_list = []  # list of list [[TCPNode('localhost:4321'), 'test1', 'leader'], ...]
    for node in node_list:
        pprint.pprint(node.getStatus())  # 命令行格式化输出
        node_self_DB_Role = []  # 存储TCPNode(Socket)节点，存储节点DB名
        node_self_DB_Role.append(node.get_self())  # self
        node_self_DB_Role.append(node.get_db_name())  # DB name
        node_self_DB_Role.append(node.get_role())  # Node Role
        node_self_DB_Role_list.append(node_self_DB_Role)
    put_table(node_self_DB_Role_list, header=['Node', 'DB', 'Role'])

    # Client只能与主节点leader进行交互，访问到follower->重定向到leader，leader与其他Nodes(followers)由Raft保持关系
    put_button('编辑节点', onclick=lambda: toast('请选择要编辑的SQLite节点'), color='info')
    node_selected_socket = user_choose_node(node_list=node_list)
    node_socket_map = node_socket_mapping(node_list)  # 获取socket与node的映射关系
    # 向用户展示选择的node状态
    put_markdown('您选择的Node的Socket为 **' + node_selected_socket + '**')
    node_selected = node_socket_map[node_selected_socket]  # 用户选择的节点
    put_markdown('您选择的Node的State为 **' + node_selected.get_role() + '**')
    put_markdown('您选择的Node的DB为 **' + node_selected.get_db_name() + '.db**')

    # 判断是否是Leader节点，不是的话要重定向到Leader节点
    # web客户端只能与Leader server进行交互  底层：Leader AppendEntry同步操作日志给其他follower
    # web用户选择follower-重定向到leader-让Client直接和leader交互-leader同步操作日志给follower
    node_redirect = None  # 重定向到的节点
    leader = None  # Leader
    if node_selected.get_role() == 'leader':
        popup('SQLite节点选择', '您选择的节点为Leader，可以直接与其交互')
        put_markdown('## 您选择的节点为Leader，可以直接与其交互,节点DB: **{}.db**'.format(node_selected.get_db_name()))
        # 打印 Leader 节点的信息
        put_node_info(node_selected)
        leader = node_selected
    else:  # follower
        popup('SQLite节点选择', '您选择的节点非Leader，服务器将进行重定向')
        time.sleep(5)
        close_popup()
        for node in node_list:
            if node.get_role() == 'leader':
                node_redirect = node
                put_markdown('## 重定向到leader: **{}**, 节点DB: **{}.db**'.format(node.get_self(), node.get_db_name()))
                # 打印重定向到节点（Leader）的信息
                put_node_info(node_redirect)
                leader = node_redirect
                break

    # 检测如果任意sqlite节点有表->删除->建立干净的实验环境
    put_markdown('## 删除此Raft集群所有节点所有表->建立干净的实验环境')
    put_text('😐Deleting......')
    for node in node_list:
        put_text('in', node.get_db_name(), '.db')
        put_text('该DB有表：', node.get_all_tb())
        if len(node.get_all_tb()):
            print('删除node{}的所有表'.format(node.getStatus()['self']))
            node.drop_table_all(whether_add=False)  # 不添加到节点日志中
    put_text('😃Down!')

    # 打印支持的操作DDL,DML,DCL
    show_operation()

    # 对Leader执行操作
    put_markdown('## 请选择对Leader进行的sql操作')
    sql = radio('choose one SQL query', options=['CREATE TABLE', 'ALTER TABLE RENAME TO', 'ALTER TABLE ADD COLUMN',
                                        'ALTER TABLE DROP COLUMN menthod1', 'ALTER TABLE DROP COLUMN menthod2',
                                           'DROP TABLE', 'DROP TABLE ALL', 'INSERT', 'INSERT MANY', 'UPDATE', 'DELETE',
                                           'SELECT', 'GRANT', 'REVOKE'])
    put_text(sql)

    # leader执行sql操作，并将log_entry同步给follower(add_log_entry)
    table_info = init_table_info()
    leader.create_table('leader_tb1', table_info)  # 创建表，在DBeaver中查看


    time.sleep(3)  # 等待日志复制 @replicated函数修饰器是异步调用的 Function will be called asynchronously
    # 展示某时间点某Node的__log_entry_queue
    print('*' * 10, '日志复制 前 各节点日志队列', '*' * 10)
    for node in node_list:
        node.show_log_entry()

    # 已废弃的日志复制方式
    # node1.replicated_log_entry()  # 进行日志复制
    # node1.replicated_log_entry_manual([node2, node3])  # 进行日志复制
    time.sleep(3)
    print('*' * 10, '日志复制 后 各节点日志队列', '*' * 10)
    node1.show_log_entry()
    node2.show_log_entry()
    node3.show_log_entry()

