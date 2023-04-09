# -*- encoding-UTF-8 -*-
__author__ = 'zhangzihao_19170100067'
__title__ = 'test2'
__desc__ = '''
learning pysyncobj and pywebio
'''

import os
import time
import threading
from pywebio.input import *
from pywebio.output import *
from pysyncobj import SyncObj, SyncObjConf
from Utils.utils import in_out_log, log_folder, slogan_path

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
    print(input)
    put_text('%r' % input)

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
        myprint(sync_pool.getStatus())
        f.write(str(sync_pool.getStatus()) + '\n')

# 进入页面提示
img = open(file=slogan_path, mode='rb').read()
put_image(img)
# put_image('https://www.python.org/static/img/python-logo.png')  # Image Output
input1 = input('This is label', type=TEXT, placeholder='This is placeholder',
        help_text='This is help text', required=True)  # 输入默认可以为空，如果需要用户必须提供值，则需要在输入函数中传入required=True

# data拿到输入项数据，类型为dict
data = input_group('Raft Node',[
    input('Input Node1:', name='node1', type=TEXT, validate=check_loaclhost_legal),
    input('Input Node2:', name='node2', type=TEXT, validate=check_loaclhost_legal),
    input('Input Node3:', name='node3', type=TEXT, validate=check_loaclhost_legal),
], cancelable=False)  # cancelable=False表单不可取消 返回dict 键为输入项的 name 值，字典值为输入项的值

# 弹窗显示Leader Electing
popup(title='Leader Electing', content=[
    put_html('<h4>⏰ in 5 seconds... </h4>'),
    put_text('🚀 building Raft nodes:'),
    put_table(tdata = [
                ['node1', data['node1']],
                ['node2', data['node2']],
                ['node3', data['node3']]
    ], header = ['Raft Node', 'ip_port']),
    put_text(data)
])
# 5s后关闭弹窗
time.sleep(5)
close_popup()

# 建立节点集群
conf = SyncObjConf(dynamicMembershipChange = True)  # 集群默认不允许动态添加节点，除非在创建时进行配置
sync_pool1 = SyncObj(data['node1'], [data['node2'], data['node3']])
sync_pool2 = SyncObj(data['node2'], [data['node1'], data['node3']])
sync_pool3 = SyncObj(data['node3'], [data['node1'], data['node2']])

# 主节点选举，3node并行进行，并行记录选举日志
myprint('-'*20 + 'Leader Electioning please wait...' + '-'*20)
t1 = threading.Thread(target=voting, args=('node1_log', sync_pool1))
t2 = threading.Thread(target=voting, args=('node2_log', sync_pool2))
t3 = threading.Thread(target=voting, args=('node3_log', sync_pool3))
t1.start()
t2.start()
t3.start()
t3.join(5)  # 3个子线程已开始执行，t3开启的最晚，所以等待t3执行完毕再解除阻塞，t3执行完毕前阻塞主线程
myprint('-'*20 + 'Leader Election over' + '-'*20)

# 展示3个节点的状态变换


# 展示leader election阶段的日志
content_dic = {}  # 表格内容物dict
count = 1
for node_log in os.listdir(log_folder):
    content = open(os.path.join(log_folder, node_log), 'rb').read()
    content_dic['node'+str(count)] = content
    count += 1
put_table(tdata = [
    ['node1', put_file(name='node1_log', content=content_dic['node1'])],
    ['node2', put_file(name='node2_log', content=content_dic['node2'])],
    ['node3', put_file(name='node3_log', content=content_dic['node3'])],
    ], header = ['Node', 'Node_log'])


