__author__ = 'zhangzihao_19170100067'
__title__ = 'webIoSql'
__desc__ = '''
提供web端与SQLite数据库交互的方法，可以让用户在web客户端操作服务器Node上的SQLite数据库
'''

from Utils.RaftSyncedSQLiteDB import RaftSyncedSQLiteDB
from Utils.utils import init_table_info
from pywebio.input import *
from pywebio.output import *

class webIoSql(object):
    ''' 提供SQLite DDL,DML,DCL的web端操作接口，web客户端操作与服务器执行操作之间的中间件

    Attributes:

    '''
    def __init__(self, node:RaftSyncedSQLiteDB):
        self.node = node  # web端要操作的server节点

    def web_execute_sql(self):
        ''' web客户端操作Raft集群的SQLite节点

        '''


class DDL(webIoSql):
    ''' 提供SQLite DDL的web端操作接口
    CREATE TABLE
    ALTER TABLE RENAME TO
    ALTER TABLE ADD COLUMN
    ALTER TABLE DROP COLUMN method1
    ALTER TABLE DROP COLUMN method2
    DROP TABLE
    DROP TABLE ALL

    Attributes:

    '''
    def __init__(self, node:RaftSyncedSQLiteDB, DDL_sql:str):
        super().__init__(node=node)
        self.DDL_sql = DDL_sql  # DDL_sql

    def web_execute_sql(self):
        ''' web客户端操作Raft集群的SQLite节点

        '''
        if self.DDL_sql == 'CREATE TABLE':
            self.web_create_table()
        elif self.DDL_sql == 'ALTER TABLE RENAME TO':
            self.web_alter_table_name()
        elif self.DDL_sql == 'ALTER TABLE ADD COLUMN':
            self.web_alter_add_column()
        elif self.DDL_sql in ['ALTER TABLE DROP COLUMN method1','ALTER TABLE DROP COLUMN method2']:
            self.web_alter_drop_column()

    def web_create_table(self):
        ''' web客户端操作Raft集群的SQLite节点-创建新表

        '''
        table_name = input('要新建的表名', type=TEXT, placeholder='请输入表名，如tb1', required=True)
        whether_auto_init_table_info = checkbox("Auto_init_table_info?", options=['Yes', 'No'])  # 返回list
        put_text('是否自动初始化表信息：' + whether_auto_init_table_info[0])
        table_info = None  # 要新建的表结构 column,datatype,isnull
        if whether_auto_init_table_info[0] == 'Yes':  # 比较value用== 比较内存地址identity用is
            table_info = init_table_info()
        else:
            # TODO 用户选择非自动化初始表信息时，要接收web端输入的column,datatype,isnull
            pass
        # 在SQLite数据库中创建表
        self.node.create_table(table_name, table_info)  # 创建表，在DBeaver中查看

    def web_alter_table_name(self):
        ''' web客户端操作Raft集群的SQLite节点-修改某表的名字

        '''
        old_table_name = input('旧表名', type=TEXT, placeholder='要重命名的表名', required=True)
        new_table_name = input('新表名', type=TEXT, placeholder='新表名', required=True)
        self.node.alter_table_name(old_table_name, new_table_name)

    def web_alter_add_column(self):
        ''' web客户端操作Raft集群的SQLite节点-向某表中添加某列

        '''
        table_name = input('要添加列的表', type=TEXT, placeholder='要添加列的表的名称', required=True)
        column_name = input('要添加的列的名', type=TEXT, placeholder='要添加的列的名', required=True)
        data_type = input('要添加的列的数据类型', type=TEXT, placeholder='要添加的列的数据类型', required=True)
        canNULL = select('列是否可为空', ['True', 'False'])
        canNULL = bool(canNULL)  # eval(canNULL)
        self.node.alter_add_column(table_name, column_name, data_type, canNULL)

    def web_alter_drop_column(self):
        ''' web客户端操作Raft集群的SQLite节点-从某表中删除某列

        '''
        table_name = input('要删除列的表', type=TEXT, placeholder='要删除列的表的名称', required=True)
        column_name = input('要删除的列', type=TEXT, placeholder='要删除的列的名称', required=True)
        self.node.alter_drop_column_method2(table_name, column_name)

class DML(webIoSql):
    ''' 提供SQLite DML的web端操作接口
    INSERT
    INSERT MANY
    UPDATE
    DELETE
    SELECT

    Attributes:

    '''
    def __init__(self, node:RaftSyncedSQLiteDB, DML_sql:str):
        super().__init__(node=node)
        self.DML_sql = DML_sql  # DML_sql

    def web_execute_sql(self):
        pass

    def web_insert(self):
        table_name = input('要插入行的表', type=TEXT, placeholder='要插入行的表名', required=True)
        column_data = []
        for column in self.node.get_column_list(table_name):  # list of column_name(str)
            data = input(f'请输入 {column} 列的值', type=TEXT, required=True)
            column_data.append(data)
        self.node.insert(table_name, column_data)

    def web_insert_many(self):

class DCL(webIoSql):
    ''' 提供SQLite DCL的web端操作接口
    GRANT
    REVOKE

    Attributes:

    '''
    def __init__(self, node:RaftSyncedSQLiteDB, DCL_sql:str):
        super().__init__(node=node)
        self.DCL_sql = DCL_sql  # DCL_sql

    def web_execute_sql(self):
        pass