# -*- encoding-UTF-8 -*-
__author__ = 'zhangzihao_19170100067'
__title__ = 'test2'
__desc__ = '''
Leader Election Show
learning pysyncobj and pywebio
'''

import os
import time
import threading
from pywebio.input import *
from pywebio.output import *
from pysyncobj import SyncObj, SyncObjConf
from Utils.utils import in_out_log, log_folder, slogan_path, check_loaclhost_legal, myprint, voting

if __name__ == '__main__':
    # 进入页面提示
    img = open(file=slogan_path, mode='rb').read()
    put_image(img)
    # put_image('https://www.python.org/static/img/python-logo.png')  # Image Output
    # input1 = input('This is label', type=TEXT, placeholder='This is placeholder',
    #                help_text='This is help text', required=True)  # 输入默认可以为空，如果需要用户必须提供值，则需要在输入函数中传入required=True

    put_markdown('# Leader Election Demo')
    welcome = input('welcome to zzh graduation project', type=TEXT, placeholder='input your project name',
                   help_text='数据库分布式复制中间件的设计与实现', required=True)  # 输入默认可以为空，如果需要用户必须提供值，则需要在输入函数中传入required=True

    # data拿到输入项数据，类型为dict
    data = input_group('Raft Node', [
        input('Input Node1:', name='node1', type=TEXT, validate=check_loaclhost_legal),
        input('Input Node2:', name='node2', type=TEXT, validate=check_loaclhost_legal),
        input('Input Node3:', name='node3', type=TEXT, validate=check_loaclhost_legal),
    ], cancelable=False)  # cancelable=False表单不可取消 返回dict 键为输入项的 name 值，字典值为输入项的值

    # 弹窗显示Leader Electing
    popup(title='Leader Electing', content=[
        put_html('<h4>⏰ in 5 seconds... </h4>'),
        put_text('🚀 building Raft nodes:'),
        put_table(tdata=[
            ['node1', data['node1']],
            ['node2', data['node2']],
            ['node3', data['node3']]
        ], header=['Raft Node', 'ip_port']),
        put_text(data)  # dict类型节点address
    ])
    # 5s后关闭弹窗
    time.sleep(5)
    close_popup()

    # 建立节点集群
    conf = SyncObjConf(dynamicMembershipChange=True)  # 集群默认不允许动态添加节点，除非在创建时进行配置
    sync_pool1 = SyncObj(data['node1'], [data['node2'], data['node3']])
    sync_pool2 = SyncObj(data['node2'], [data['node1'], data['node3']])
    sync_pool3 = SyncObj(data['node3'], [data['node1'], data['node2']])

    # 主节点选举，3node并行进行，并行记录选举日志入log_folder
    myprint('-' * 20 + 'Leader Electioning please wait...' + '-' * 20)
    t1 = threading.Thread(target=voting, args=('node1_log', sync_pool1))  # 主节点选举过程封装写日志逻辑
    t2 = threading.Thread(target=voting, args=('node2_log', sync_pool2))
    t3 = threading.Thread(target=voting, args=('node3_log', sync_pool3))
    t1.start()
    t2.start()
    t3.start()
    t3.join(5)  # 3个子线程已开始执行，t3开启的最晚，所以等待t3执行完毕再解除阻塞，t3执行完毕前阻塞主线程
    myprint('-' * 20 + 'Leader Election over' + '-' * 20)

    # 展示3个节点的状态变换

    # 展示leader election阶段的日志
    content_dic = {}  # 表格内容物dict
    count = 1
    for node_log in os.listdir(log_folder):
        content = open(os.path.join(log_folder, node_log), 'rb').read()
        content_dic['node' + str(count)] = content
        count += 1
    put_table(tdata=[
        ['node1', put_file(name='node1_log.txt', content=content_dic['node1'])],
        ['node2', put_file(name='node2_log.txt', content=content_dic['node2'])],
        ['node3', put_file(name='node3_log.txt', content=content_dic['node3'])],
    ], header=['Node', 'Node_log'])


