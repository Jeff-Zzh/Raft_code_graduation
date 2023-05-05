# -*- encoding-UTF-8 -*-
__author__ = 'zhangzihao_19170100067'
__title__ = 'utilities for project'
__desc__ = '''
learning pysyncobj and pywebio
'''
import functools
from pywebio.input import *
from pywebio.output import *

log_folder = r'E:\Raft\Raft-code\log'
slogan_path = r'E:\Raft\Raft-code\document\slogan.png'

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


if __name__ == '__main__':
    @in_out_log
    def fun1():
        print('i am fun1()')
    fun1()