# -*- encoding-UTF-8 -*-
__author__ = 'zhangzihao_19170100067'
__title__ = 'utilities for project'
__desc__ = '''
learning pysyncobj and pywebio
'''
import functools

log_folder = r'E:\Raft\Raft-code\log'

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

if __name__ == '__main__':
    @in_out_log
    def fun1():
        print('i am fun1()')
    fun1()