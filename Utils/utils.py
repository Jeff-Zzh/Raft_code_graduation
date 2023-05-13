# -*- encoding-UTF-8 -*-
__author__ = 'zhangzihao_19170100067'
__title__ = 'utilities for project'
__desc__ = '''
using pysyncobj and pywebio or other site-packages to provide utilities for the whole project
'''

import functools
from pywebio.input import *
from pywebio.output import *

# log_folder = r'E:\Raft\Raft-code\log'
# slogan_path = r'E:\Raft\Raft-code\document\slogan.png'
log_folder = r'.\log'  # 改变路径依赖 当前工作目录cwd是E:\Raft 使用log文件夹的相对路径
slogan_path = r'.\document\slogan.png'  # 改变路径依赖

def in_out_log(f):
    ''' 进出函数记录

    :param f:
    :return:
    '''
    @functools.wraps(f)
    def wrap(*args, **kwargs):
        print('-'*20, 'in ' + f.__name__, '-'*20)
        res = f(*args, **kwargs)
        print('-'*20, 'out ' + f.__name__, '-'*20)
        return res  # 被修饰函数的返回值
    return wrap

def web_sql_hint(f):
    ''' web端操作数据库函数接口提示

    '''
    @functools.wraps(f)
    def wrap(*args, **kwargs):
        put_text('-'*20, 'in ' + f.__name__, '-'*20)
        return f(*args, **kwargs)
    return wrap

def check_loaclhost_legal(ip_port):
    ''' 本地Raft节点ip，port格式正确性输入校验

    注： 不要随便return,validate参数接收此变量检查函数，只要有return，就说明input输入项的值有误，就不能正常提交表单进入下一步了
    :param ip_port: E.g. 127.0.0.1:8080
    :return: 只返回错误情况的输出信息： '本地环回地址格式有误'
    '''
    ip_port = str(ip_port)
    ip = ip_port.split(':')[0]
    port = ip_port.split(':')[1]
    ip_split_list = ip.split('.')
    if len(ip_split_list) == 4 and ip_split_list[0] == '127':
        # return '本地环回地址格式正确'
        pass
    else:
        return '本地环回地址格式有误'

def myprint(input):
    ''' web 与 命令行同时打印

    :param input: 打印文本
    :return: None
    '''
    print(input)  # 命令行打印
    put_text('%r' % input)  # web打印

@in_out_log
def voting(log_name, sync_pool):
    ''' 投票选举主节点，日志记录，日志打印

    主节点选取日志打印、记录
    :param log_name: 某node的日志文件名
    :param sync_pool: 某node
    :return:
    '''
    with open(log_folder + f'\{log_name}.txt', 'w+') as f:  # 写日志
        while(sync_pool.getStatus()['leader'] is None):
            node_state = sync_pool.getStatus()
            # myprint(time.time())
            # myprint(node_state)  选举过程
            f.write(str(node_state) + '\n')  # 写日志
        # leader选举完毕
        put_text('final state of node {}'.format(sync_pool.getStatus()['self'].address))
        myprint(sync_pool.getStatus())  # web 命令行
        f.write(str(sync_pool.getStatus()) + '\n')  # 日志

def init_table_info():
    ''' 初始化表格信息，并在web端展示

    '''
    # table info
    myprint('自动化初始化表信息 Show New Table Info')
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


if __name__ == '__main__':
    @in_out_log
    def fun1():
        print('i am fun1()')
    fun1()