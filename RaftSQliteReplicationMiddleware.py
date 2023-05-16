import time
import logging  # 用于命令行格式化日志输出
import pprint  # 用于命令行格式化打印
from pywebio.input import *
from pywebio.output import *

from Utils.utils import in_out_log, slogan_path, myprint  # 公共工具
from Utils.RaftSyncedSQLiteDB import RaftSyncedSQLiteDB  # 服务器Node 写入DB
from Utils.webIoSql import webIoSql, DDL, DML, DCL  # 客户端web_io方法

# logging默认的日志级别是WARNING 只有级别大于或等于WARNING的日志消息才会被处理，低于该级别的消息将被忽略
logging.basicConfig(level=logging.INFO)  # 设置日志级别为INFO, 打印级别为INFO的日志消息


def build_raft_cluster(node_to_be_build):
    ''' 初始化/构建 遵守raft协议的SQLite服务器节点集群

    node格式：键值对 {'localhost:4321':'test1'}

    Args:
        node_to_be_build:要初始化的集群list of dic [{'localhost:4321':'test1}]
    Returns:
        node_list:构建好的基于Raft协议的SQLite数据库节点集群
    '''
    node_list = []  # 构建好的node存放的list
    for node_index in range(len(node_to_be_build)):
        other_node_list = node_to_be_build[:node_index] + node_to_be_build[node_index+1:]  # node_index下标的node的其他节点list
        other_node_address = []  # 其他节点地址list
        for other_node in other_node_list:
            other_node_address.append(list(other_node.keys())[0])
        node = RaftSyncedSQLiteDB(list(node_to_be_build[node_index].keys())[0],  # selfNodeAddr
                                    other_node_address,  # partnerNodeAddrs
                                    list(node_to_be_build[node_index].values())[0] )  # db_name list()[0]把dict_keys类转为str类
        node_list.append(node)
    return node_list

def init_web():
    ''' 初始化web界面，展示对应DIY界面

    '''
    img = open(file=slogan_path, mode='rb').read()  # 二进制读图片文件，put_image
    put_image(img)
    put_markdown('# Log Replication Demo With SQLite')


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
    ''' 返回dict，保存socket和其对应node的映射关系{ {'localhost:4321:node1'} }

    :param node_list:list of RaftSyncedSQLiteDB类实例化

    Returns:
        Dict:node和其socket的映射关系{ {'localhost:4321:node1'} }
    '''
    node_socket_map = {}
    for node in node_list:
        node_socket_map[str(node.get_self())] = node
    return node_socket_map

def put_node_info(node):
    ''' web端打印node节点信息 table,log_entry,...待扩展

    '''
    put_text('节点SQLite表：', node.get_all_tb())
    put_text('该节点日志条目Log Entry', node.get_log_entry())

def show_operation():
    ''' 展示Raft-SQLite集群支持的web交互功能

    '''
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

def web_sql():
    ''' web端操作Raft-SQLite集群某节点

    '''
    click_flag = False
    def click():
        nonlocal click_flag  # 使用 nonlocal 关键字来指示 click() 函数引用外部函数的变量
        click_flag = True

    put_button("点击停止操作leader节点", onclick=lambda: click(), color='success', outline=True)
    while not click_flag:  # 如果web端没有点击button，就继续循环操作leader节点
        # 选择sql类型
        sql_type = radio("Choose SQL type", options=['DDL', 'DML', 'DCL'])
        put_text('sql_type = %r' % sql_type)

        # 选择可以对Leader执行的sql操作
        put_markdown(f'## 请选择对Leader进行的{sql_type} sql操作')
        sql = ''  # sql语句
        if sql_type == 'DDL':
            sql = radio('Choose One DDL_SQL query', options=['CREATE TABLE',
                                                             'ALTER TABLE RENAME TO',
                                                             'ALTER TABLE ADD COLUMN',
                                                             'ALTER TABLE DROP COLUMN method1',
                                                             'ALTER TABLE DROP COLUMN method2',
                                                             'DROP TABLE', 'DROP TABLE ALL'])
        elif sql_type == 'DML':
            sql = radio('Choose One DML_SQL query', options=['INSERT', 'INSERT MANY',
                                                             'UPDATE',
                                                             'DELETE',
                                                             'SELECT'])
        elif sql_type == 'DCL':
            sql = radio('Choose One DCL_SQL query', options=['GRANT', 'REVOKE'])
        put_text(sql)

        # 初始化webIoSql的子类DDL,DML,DCL，使其连接Raft集群中的leader Node，提供SQLite DDL,DML,DCL的web端操作接口
        ddl = dml = dcl = None  # python连等将多个变量绑定到同一个值，它们都指向同一个内存位置
        if sql_type == 'DDL':
            ddl = DDL(node=leader, DDL_sql=sql)
            # web_io_leader传递web端用户的操作 给 服务器leader,leader执行sql操作，并将log_entry同步给follower(add_log_entry)
            ddl.web_execute_sql()  # 执行对应web_ddl语句，在DBeaver中查看db的变化
        elif sql_type == 'DML':
            dml = DML(node=leader, DML_sql=sql)
            dml.web_execute_sql()  # 执行对应web_dml语句，在DBeaver中查看db的变化
        elif sql_type == 'DCL':
            dcl = DCL(node=leader, DCL_sql=sql)
            dcl.web_execute_sql()  # 执行对应web_dcl语句，在DBeaver中查看db的变化


def test():
    ''' 开发代码时，测试用函数

    '''
    column_name_list = ['column1', 'column2', 'column3']
    datatype_list = ['TEXT', 'INT', 'CHAR(50)']
    isNULL_list = ['NOT NULL', 'NOT NULL', 'NOT NULL']
    table_info = {'column': column_name_list, 'datatype': datatype_list, 'isnull': isNULL_list}
    node1 = RaftSyncedSQLiteDB('localhost:4321', ['localhost:4322', 'localhost:4323'], 'test1')  # test1.db
    node2 = RaftSyncedSQLiteDB('localhost:4322', ['localhost:4321', 'localhost:4323'], 'test2')  # test2.db
    node3 = RaftSyncedSQLiteDB('localhost:4323', ['localhost:4321', 'localhost:4322'], 'test3')  # test3.db
    node_list = [node1, node2, node3]
    node1.create_table('tb1',table_info)
    node1.alter_table_name('tb1', 'new_tb1')
    node1.alter_add_column('new_tb1', 'column_alter', 'VARCHAR(50)', canNULL=False)  # 不允许此列的值为空
    # node1.alter_drop_column_method1('new_tb1', 'column1')  版本原因，用不了
    node1.alter_drop_column_method2('new_tb1', 'column1')
    put_text('get table info', node1.get_table_info('new_tb1'))  # 获取表信息
    put_text('获取某表列的信息', node1.get_column_list('new_tb1'))
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

def web_DDL_test():
    ''' 功能测试-web操作DDL语句

    test_steps:
        connect Raft_SQLite_node1-->web_create_table-->web_alter_table_name-->web_alter_add_column
        -->web_alter_drop_column-->web_drop_table-->web_drop_table_all-->show changes in DBeaver
    5.13 测试 web端DDL语句所有功能执行无问题
    '''
    node1 = RaftSyncedSQLiteDB('localhost:4321', ['localhost:4322', 'localhost:4323'], 'test1')  # test1.db
    node1.drop_table_all()  # 删除该节点所有表，每次执行前后不用手动去DBeaver中删除表了
    ddl = DDL(node1, 'CREATE TABLE')
    ddl.web_execute_sql()
    ddl = DDL(node1, 'ALTER TABLE RENAME TO')
    ddl.web_execute_sql()
    ddl = DDL(node1, 'ALTER TABLE ADD COLUMN')
    ddl.web_execute_sql()
    ddl = DDL(node1, 'ALTER TABLE DROP COLUMN')
    ddl.web_execute_sql()
    ddl = DDL(node1, 'DROP TABLE')
    ddl.web_execute_sql()
    for _ in range(2):
        ddl = DDL(node1, 'CREATE TABLE')
        ddl.web_execute_sql()
    ddl = DDL(node1, 'DROP TABLE ALL')  # 需要该节点有table才能进行drop操作
    ddl.web_execute_sql()

def web_DML_test():
    ''' 功能测试-web操作DML语句

    test_steps:
        connect Raft_SQLite_node1-->web_create_table-->web_insert-->web_insert_many-->web_update
        -->web_delete-->web_select-->show changes in DBeaver
    5.13 测试 web端DML语句所有功能执行无问题
    '''
    node1 = RaftSyncedSQLiteDB('localhost:4321', ['localhost:4322', 'localhost:4323'], 'test1')  # test1.db
    node1.drop_table_all()  # 删除该节点所有表，每次执行前后不用手动去DBeaver中删除表了
    ddl = DDL(node1, 'CREATE TABLE')
    ddl.web_execute_sql()
    dml = DML(node1, 'INSERT')
    dml.web_execute_sql()
    # node1.insert_many('tb1', [(1,1,1), (2,2,2)])  手动inert_many
    dml = DML(node1, 'INSERT MANY')
    dml.web_execute_sql()
    dml = DML(node1, 'UPDATE')
    dml.web_execute_sql()
    dml = DML(node1, 'DELETE')
    dml.web_execute_sql()
    dml = DML(node1, 'SELECT')
    dml.web_execute_sql()

def web_DCL_test():
    ''' 功能测试-web操作DCL语句

    test_steps:
        connect Raft_SQLite_node1-->web_create_table-->web_grant-->web_revoke-->show changes in DBeaver
    5.13 测试 web端DCL语句所有功能执行无问题
    '''
    node1 = RaftSyncedSQLiteDB('localhost:4321', ['localhost:4322', 'localhost:4323'], 'test1')  # test1.db
    node1.drop_table_all()  # 删除该节点所有表，每次执行前后不用手动去DBeaver中删除表了
    ddl = DDL(node1, 'CREATE TABLE')
    ddl.web_execute_sql()
    dcl = DCL(node1, 'GRANT')
    dcl.web_execute_sql()
    dcl = DCL(node1, 'REVOKE')
    dcl.web_execute_sql()

def web_sql_test():
    # 单节点web_sql功能测试
    web_DDL_test()
    web_DML_test()
    web_DCL_test()

if __name__ == '__main__':
    # TODO 性能测试 写/s 中间件加入前后 可用性测试
    # test()
    # web_sql_test()

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

    node_list = build_raft_cluster(node_to_be_build=node_to_be_build)  # Raft集群构建函数

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
        put_markdown('## 您选择的节点为Leader，可以直接与其交互，节点DB: **{}.db**'.format(node_selected.get_db_name()))
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

    # 打印web端支持的sql操作DDL,DML,DCL
    show_operation()

    # 在web端循环操作leader节点
    web_sql()

    time.sleep(3)  # 等待日志复制 @replicated函数修饰器是异步调用的 Function will be called asynchronously

    # 展示某时间点某Node的__log_entry_queue
    put_markdown('## 展示Raft集群节点日志信息')
    myprint('*' * 10 + ' 各节点日志队列 ' + '*' * 10)
    myprint('当前时间:{}'.format(time.strftime('%Y-%m-%d-%H-%M-%S', time.localtime())))
    for node in node_list:
        node.show_log_entry()

    # 操作日志应用，实现在leader操作，同步到其他非leader节点，在DBeaver中查看
    whether_execute_log = radio('手动操作Raft步骤：其他非Leader节点是否 执行 同步的操作日志？', options=['Yes', 'No'])
    if whether_execute_log:
        for node in node_list:
            node.execute_log_entry()
        toast('同步的操作日志 执行完毕，请在DBeaver中核验结果')
        put_markdown('## 同步的操作日志 执行完毕，请在DBeaver中核验结果')
    else:
        put_markdown('## 您未选择 执行 同步的操作日志，将不会在其他非leader节点上看到leader节点的操作结果')
