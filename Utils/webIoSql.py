__author__ = 'zhangzihao_19170100067'
__title__ = 'webIoSql'
__desc__ = '''
提供web端与SQLite数据库交互的方法，可以让用户在web客户端操作服务器Node上的SQLite数据库
'''

from Utils.RaftSyncedSQLiteDB import RaftSyncedSQLiteDB
from Utils.utils import init_table_info, web_sql_hint
from pywebio.input import *
from pywebio.output import *

class webIoSql(object):
    ''' 提供SQLite DDL,DML,DCL的web端操作接口，web客户端操作与服务器执行操作之间的中间件

    Attributes:
        node:遵从Raft协议的分布式数据库集群的某节点
        DDL_sql:数据定义语言 语句
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
        elif self.DDL_sql in ['ALTER TABLE DROP COLUMN method1',
                              'ALTER TABLE DROP COLUMN method2',
                              'ALTER TABLE DROP COLUMN']:
            self.web_alter_drop_column()
        elif self.DDL_sql == 'DROP TABLE':
            self.web_drop_table()
        elif self.DDL_sql == 'DROP TABLE ALL':
            self.web_drop_table_all()

    @web_sql_hint
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

    @web_sql_hint
    def web_alter_table_name(self):
        ''' web客户端操作Raft集群的SQLite节点-修改某表的名字

        '''
        put_text(f'当前Node {self.node.get_self()} 有表 {self.node.get_all_tb()}')
        old_table_name = input('旧表名', type=TEXT, placeholder='要重命名的表名', required=True)
        new_table_name = input('新表名', type=TEXT, placeholder='新表名', required=True)
        self.node.alter_table_name(old_table_name, new_table_name)
        put_text(f'当前Node {self.node.get_self()} 有表 {self.node.get_all_tb()}')

    @web_sql_hint
    def web_alter_add_column(self):
        ''' web客户端操作Raft集群的SQLite节点-向某表中添加某列

        '''
        put_text(f'当前Node {self.node.get_self()} 有表 {self.node.get_all_tb()}')
        table_name = input('要添加列的表', type=TEXT, placeholder='要添加列的表的名称', required=True)
        put_text(f'该表有 {self.node.get_column_len(table_name)} 列: {self.node.get_column_list(table_name)}')
        column_name = input('要添加的列的名', type=TEXT, placeholder='要添加的列的名', required=True)
        data_type = input('要添加的列的数据类型', type=TEXT, placeholder='NULL, INTEGER, REAL, TEXT, BLOB', required=True)
        canNULL = select('列是否可为空', ['True', 'False'])
        canNULL = bool(canNULL)  # eval(canNULL)
        self.node.alter_add_column(table_name, column_name, data_type, canNULL)
        put_text(f'该表有 {self.node.get_column_len(table_name)} 列: {self.node.get_column_list(table_name)}')

    @web_sql_hint
    def web_alter_drop_column(self):
        ''' web客户端操作Raft集群的SQLite节点-从某表中删除某列

        '''
        put_text(f'当前Node {self.node.get_self()} 有表 {self.node.get_all_tb()}')
        table_name = input('要删除列的表', type=TEXT, placeholder='要删除列的表的名称', required=True)
        put_text(f'该表有 {self.node.get_column_len(table_name)} 列: {self.node.get_column_list(table_name)}')
        column_name = input('要删除的列', type=TEXT, placeholder='要删除的列的名称', required=True)
        self.node.alter_drop_column_method2(table_name, column_name)
        put_text(f'该表有 {self.node.get_column_len(table_name)} 列: {self.node.get_column_list(table_name)}')

    @web_sql_hint
    def web_drop_table(self):
        ''' web客户端操作Raft集群的SQLite节点-drop剔除某表，不是删除表的数据，是直接整个表都被删除

        '''
        put_text(f'当前Node {self.node.get_self()} 有表 {self.node.get_all_tb()}')
        table_name = input('要drop剔除的表', type=TEXT, placeholder='要剔除的表的名称', required=True)
        yes_or_no = radio('确定要drop该表吗？', options=['是', '否'])
        if yes_or_no == '是':
            put_text('您选择 {}，将进行drop {}操作'.format(yes_or_no, table_name))
            self.node.drop_table(table_name)
        elif yes_or_no == '否':
            put_text('您选择 {}，不会进行drop {}操作'.format(yes_or_no, table_name))
        put_text(f'当前Node {self.node.get_self()} 有表 {self.node.get_all_tb()}')

    @web_sql_hint
    def web_drop_table_all(self):
        ''' web客户端操作Raft集群的SQLite

        节点-剔除此Node的db的所有表

        '''
        put_text(f'当前Node {self.node.get_self()} 有表 {self.node.get_all_tb()}')
        yes_or_no = radio('确定要剔除此Node的db的所有表吗？', options=['是', '否'])
        if yes_or_no == '是':
            put_text('您选择{}，将进行drop_table_all操作'.format(yes_or_no))
            self.node.drop_table_all()
        elif yes_or_no == '否':
            put_text('您选择 {}，不会进行drop_table_all操作'.format(yes_or_no))
        put_text(f'当前Node {self.node.get_self()} 有表 {self.node.get_all_tb()}')


class DML(webIoSql):
    ''' 提供SQLite DML的web端操作接口
    INSERT
    INSERT MANY
    UPDATE
    DELETE
    SELECT

    Attributes:
        node:遵从Raft协议的分布式数据库集群的某节点
        DML_sql:数据操纵语言 语句
    '''
    def __init__(self, node:RaftSyncedSQLiteDB, DML_sql:str):
        super().__init__(node=node)
        self.DML_sql = DML_sql  # DML_sql

    def web_execute_sql(self):
        ''' web客户端操作Raft集群的SQLite节点

        '''
        if self.DML_sql == 'INSERT':
            self.web_insert()
        elif self.DML_sql == 'INSERT MANY':
            self.web_insert_many()
        elif self.DML_sql == 'UPDATE':
            self.web_update()
        elif self.DML_sql == 'DELETE':
            self.web_delete()
        elif self.DML_sql == 'SELECT':
            self.web_select()

    @web_sql_hint
    def web_insert(self):
        ''' web客户端操作Raft集群的SQLite节点-向某表中插入一条数据

        '''
        put_text(f'当前Node {self.node.get_self()} 有表 {self.node.get_all_tb()}')
        table_name = input('要插入行的表', type=TEXT, placeholder='要插入行的表名', required=True)
        put_text(f'该表有 {self.node.get_column_len(table_name)} 列: {self.node.get_column_list(table_name)}')
        column_data = []
        for column in self.node.get_column_list(table_name):  # list of column_name(str)
            data = input(f'请输入 {column} 列的值', type=TEXT, required=True)
            column_data.append(data)
        self.node.insert(table_name, column_data)

    @web_sql_hint
    def web_insert_many(self):
        ''' web客户端操作Raft集群的SQLite节点-向某表中插入多条数据

        '''
        put_text(f'当前Node {self.node.get_self()} 有表 {self.node.get_all_tb()}')
        table_name = input('要插入多行的表', type=TEXT, placeholder='要插入多行的表名', required=True)
        put_text(f'该表有 {self.node.get_column_len(table_name)} 列: {self.node.get_column_list(table_name)}')
        row_num = input('你想insert几行？', type=TEXT, placeholder='要插入的行数', required=True)
        row_num = int(row_num)

        data_many = []  # 要插入的数据 list of tuple [(),(),...]
        for row in range(row_num):
            put_text(f'这是要插入的第 {row+1} 行')
            column_data = []
            for column in self.node.get_column_list(table_name):  # list of column_name(str)
                data = input(f'第 {row + 1} 行，请输入 {column} 列的值', type=TEXT, required=True)
                column_data.append(data)
            column_data = tuple(column_data)
            data_many.append(column_data)

        self.node.insert_many(table_name, data_many)

    @web_sql_hint
    def web_update(self):
        ''' web客户端操作Raft集群的SQLite节点-修改某表中已有行的某一列的值

        '''
        put_text('注：想要update某表，那该表一定要先有 >=1 行才能进行 update 操作')
        put_text(f'当前Node {self.node.get_self()} 有表 {self.node.get_all_tb()}')
        table_name = input('要update的表', type=TEXT, placeholder='要update的表名', required=True)
        put_text(f'该表有 {self.node.get_column_len(table_name)} 列: {self.node.get_column_list(table_name)}')
        set_column = input('要SET值的列', type=TEXT, placeholder='要SET值的列名', required=True)
        value = input('要给set_column列的值', type=TEXT, placeholder='要给set_column列的值', required=True)
        where = radio('是否要有筛选条件?', options=['True', 'False'])
        condition = None
        if eval(where):  # 添加筛选条件
            condition = input('请输入筛选条件', type=TEXT,
                              placeholder='筛选条件 如 ID = 6  col1 = \'some_value\'  值是str在sql中要加单引号',
                              required=True)
        self.node.update(table_name, set_column, value, eval(where), condition)  # bug fix:where要str转bool

    @web_sql_hint
    def web_delete(self):
        ''' web客户端操作Raft集群的SQLite节点-删除某表已有的记录

        用带有 WHERE 子句的 DELETE 查询来删除选定行，否则所有的记录都会被删除
        '''
        table_name = input('要delete行的表', type=TEXT, placeholder='要delete行的表名', required=True)
        put_text(f'该表当前状态: {self.node.get_table_info(table_name)}')
        where = radio(f'是否要有筛选条件？无筛选条件的话，{table_name} 所有的行都会被删除', options=['True', 'False'])
        condition = None
        if bool(where):  # 添加筛选条件
            condition = input('请输入筛选条件', type=TEXT,
                              placeholder='筛选条件 如 ID = 6  col1 = \'some_value\'  值是str在sql中要加单引号',
                              required=True)
        self.node.delete(table_name, eval(where), condition)  # bug fix:where要str转bool

    @web_sql_hint
    def web_select(self):
        ''' web客户端操作Raft集群的SQLite节点-从 SQLite 数据库表中获取数据，以结果表的形式返回数据

        '''
        table_name = input('要select的表', type=TEXT, placeholder='要select的表名', required=True)
        whether_select_all = radio(f'select {table_name}表中所有行？', options=['Yes', 'No'])
        column_list = None
        if whether_select_all == 'Yes':
            column_list = '*'
        else:
            column_list = []
            put_text(f'该表有 {self.node.get_column_len(table_name)} 列: {self.node.get_column_list(table_name)}')
            column_list = checkbox('请选择你想从表中获取的字段（列）', options=self.node.get_column_list(table_name))

        put_text(self.node.select(table_name, column_list))


class DCL(webIoSql):
    ''' 提供SQLite DCL的web端操作接口
    GRANT
    REVOKE

    Attributes:
        node:遵从Raft协议的分布式数据库集群的某节点
        DCL_sql:数据控制语言 语句

    在SQLite中，GRANT语句只能在特定的条件下使用。根据SQLite的文档，GRANT语句只在SQLite的服务器版本中可用，
    而不是在嵌入式版本（如使用Python的SQLite模块）中可用。
    由于你在执行'GRANT SELECT ON tb1 TO jack;'时遇到了语法错误，这可能是因为你正在使用的是SQLite的嵌入式版本，该版本不支持GRANT语句。

    如果你需要在SQLite中授予和撤销权限，你可能需要考虑使用SQLite的服务器版本，如SQLite-Server。这样，你将能够使用GRANT语句来授予和撤销权限。

    另一种替代方案是在你的应用程序中模拟权限系统，使用代码逻辑来限制和控制用户的访问和操作。
    这需要自行实现授权逻辑，例如在应用程序中对用户进行身份验证，并根据其角色或其他标识来控制其对数据库的访问和操作。
    '''
    def __init__(self, node:RaftSyncedSQLiteDB, DCL_sql:str):
        super().__init__(node=node)
        self.DCL_sql = DCL_sql  # DCL_sql

    def web_execute_sql(self):
        ''' web客户端操作Raft集群的SQLite节点

        '''
        if self.DCL_sql == 'GRANT':
            self.web_grant()
        elif self.DCL_sql == 'REVOKE':
            self.web_revoke()

    @web_sql_hint
    def web_grant(self):
        ''' web客户端操作Raft集群的SQLite节点-授予 用户 或 用户组 对数据库对象的权限

        '''
        permission = radio('要授予的权限类型',
                           options=['SELECT', 'INSERT', 'DELETE', 'UPDATE', 'CREATE', 'ALTER', 'DROP'])
        put_text(f'当前Node {self.node.get_self()} 有表 {self.node.get_all_tb()}')
        object = input('要授予权限的对象', type=TEXT, required=True,
                       placeholder='要授予权限的对象，可以是表、视图或其他数据库对象的名称')
        user = input('要授予权限的对象', type=TEXT, placeholder='要授予权限的用户或用户组的名称', required=True)
        self.node.grant(permission, object, user)

    @web_sql_hint
    def web_revoke(self):
        ''' web客户端操作Raft集群的SQLite节点-撤销 用户 或 用户组 对数据库对象的权限

        '''
        permission = radio('要撤销的权限类型',
                           options=['SELECT', 'INSERT', 'DELETE', 'UPDATE', 'CREATE', 'ALTER', 'DROP'])
        put_text(f'当前Node {self.node.get_self()} 有表 {self.node.get_all_tb()}')
        object = input('要撤销权限的对象', type=TEXT, required=True,
                       placeholder='要撤销权限的对象，可以是表、视图或其他数据库对象的名称')
        user = input('要撤销权限的对象', type=TEXT, placeholder='要撤销权限的用户或用户组的名称', required=True)
        self.node.revoke(permission, object, user)
