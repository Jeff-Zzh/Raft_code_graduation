import time
import sqlite3
from pywebio.input import *
from pywebio.output import *
from pysyncobj import SyncObj, SyncObjConf, replicated
from Utils.utils import in_out_log, slogan_path, myprint


class Node(SyncObj):
    def __init__(self, self_node_addr, partner_node_addrs, **kwargs):
        SyncObj.__init__(self, self_node_addr, partner_node_addrs, **kwargs)
        self.__data = ''  # collect_agent版本号

    def get_node_addr(self):  # node.selfNode 返回 127.0.0.1:4321
        return super().selfNode
    def get_conf(self):  # 返回SyncObjConf对象
        return super().conf()
    @replicated
    def set_data(self, data):
        self.__data = data
    def get_data(self):
        return self.__data

# 全局变量-标识是否点击
clicked = False

def set_ndoe_data(node, data):
    '''

    :param node: 节点
    :param data: 值
    :return:
    '''
    global clicked
    clicked = True  # 设置已点击标识为True
    node.set_data(data)  # set_data被@replicated装饰器装饰，设置某一节点值，就会开始日志复制，在其他节点同步该数据
    put_markdown(f'set **{node.selfNode}** data: **{data}**' )


if __name__ == '__main__':
    img = open(file=slogan_path, mode='rb').read()  # 二进制读图片文件，put_image
    put_image(img)
    put_markdown('# Log Replication Demo')

    put_text('-' * 20 + '建立3个节点' + '-' * 20)
    conf = SyncObjConf(dynamicMembershipChange=True)
    node1 = Node('127.0.0.1:4321', ['127.0.0.1:4322', '127.0.0.1:4323'], conf=conf)
    node2 = Node('127.0.0.1:4322', ['127.0.0.1:4321', '127.0.0.1:4323'], conf=conf)
    node3 = Node('127.0.0.1:4323', ['127.0.0.1:4321', '127.0.0.1:4322'], conf=conf)
    # 记录节点
    node_dic = {}
    node_dic['node1'] = node1
    node_dic['node2'] = node2
    node_dic['node3'] = node3

    time.sleep(5)
    put_text(node1.getStatus())
    print(node1.getStatus())

    time.sleep(1)
    put_text(node2.getStatus())
    print(node2.getStatus())
    time.sleep(1)
    put_text(node3.getStatus())
    print(node3.getStatus())
    put_text('-' * 20 + '已经建立3个节点' + '-' * 20)
    toast('🔔 Build 3 nodes successfully!')
    time.sleep(2)
    # 记录 主节点选举 后3个node的日志信息
    node_log_dic1 = {}
    node_log_dic1['node1'] = node1.getStatus()
    node_log_dic1['node2'] = node2.getStatus()
    node_log_dic1['node3'] = node3.getStatus()


    # 展示节点data值为 空
    # client执行set_data操作 -> 设置某节点的data-> 转为和leader节点进行通信 -> 设置主节点值 -> 将操作打包成日志 -> 将此日志复制给其他节点 -> 循环打印 各节点data值 -> 查看日志复制情况
    put_text(
        'client执行set_data操作 -> 设置某节点的data-> 转为和leader节点进行通信 -> 设置主节点值 -> 将操作打包成日志 -> 将此日志复制给其他节点 -> 循环打印 各节点data值 -> 查看日志复制情况')
    put_table(tdata=[
        ['node1', '127.0.0.1:4321', node1.get_data()],
        ['node2', '127.0.0.1:4322', node2.get_data()],
        ['node3', '127.0.0.1:4323', node3.get_data()]
    ], header=['Raft Node', 'address', 'data'])  # 输出空值

    # myprint(node2.get_data())
    select_node = select('请选择要设置的节点', ['Node1', 'Node2', 'Node3'])
    input_data = input("请输入要设置的节点的值", type=TEXT)
    # 点击按钮 写入data值，raft再进行主节点日志复制
    put_button(label='设置节点的data值', onclick=lambda: set_ndoe_data(node_dic[select_node.lower()], input_data),
               color='success')
    toast(f'🔔 Please click the button to set {select_node} data!')
    put_markdown(f'🔔 Please click the button to set **{select_node}** data!')
    while True:
        put_text(f'点击： {clicked}')
        if not clicked:  # 如果还没给node1赋值，就间隔打印
            time.sleep(2)  # 打印间隔
        put_table(tdata=[
            ['node1', '127.0.0.1:4321', node1.get_data(), time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())],
            ['node2', '127.0.0.1:4322', node2.get_data(), time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())],
            ['node3', '127.0.0.1:4323', node3.get_data(), time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())]
        ], header=['Raft Node', 'address', 'data', 'local time'])
        if node1.get_data() == input_data and node2.get_data() == input_data and node3.get_data() == input_data:
            break
    put_text('-' * 20 + '复制日志' + '-' * 20)
    myprint(node1.getStatus())
    myprint(node2.getStatus())
    myprint(node3.getStatus())
    put_text('-' * 20 + '复制日志' + '-' * 20)
    # 记录 日志复制 后3个node的日志信息
    node_log_dic2 = {}
    node_log_dic2['node1'] = node1.getStatus()
    node_log_dic2['node2'] = node2.getStatus()
    node_log_dic2['node3'] = node3.getStatus()

    table1 = put_table(tdata=[
        ['Node1', node_log_dic1['node1']['log_len'], node_log_dic1['node1']['last_applied'], node_log_dic1['node1']['commit_idx'],
         node_log_dic1['node1']['leader_commit_idx'], node_log_dic1['node1']['uptime']],
        ['Node2', node_log_dic1['node2']['log_len'], node_log_dic1['node2']['last_applied'], node_log_dic1['node2']['commit_idx'],
         node_log_dic1['node2']['leader_commit_idx'], node_log_dic1['node2']['uptime']],
        ['Node3', node_log_dic1['node3']['log_len'], node_log_dic1['node3']['last_applied'], node_log_dic1['node3']['commit_idx'],
         node_log_dic1['node3']['leader_commit_idx'], node_log_dic1['node3']['uptime']]
    ], header= ['Diff', 'log_len', 'last_applied', 'commit_idx', 'leader_commit_idx', 'uptime'])
    table2 = put_table(tdata=[
        [node_log_dic2['node1']['log_len'], node_log_dic2['node1']['last_applied'], node_log_dic2['node1']['commit_idx'],
         node_log_dic2['node1']['leader_commit_idx'], node_log_dic2['node1']['uptime']],
        [node_log_dic2['node2']['log_len'], node_log_dic2['node2']['last_applied'], node_log_dic2['node2']['commit_idx'],
         node_log_dic2['node2']['leader_commit_idx'], node_log_dic2['node2']['uptime']],
        [node_log_dic2['node3']['log_len'], node_log_dic2['node3']['last_applied'], node_log_dic2['node3']['commit_idx'],
         node_log_dic2['node3']['leader_commit_idx'], node_log_dic2['node3']['uptime']]
    ], header= ['log_len', 'last_applied', 'commit_idx', 'leader_commit_idx', 'uptime'])
    table0 = put_table( [['Diff'], ['Node1'], ['Node2'], ['Node3']])

    put_markdown('### 主节点选举后3节点日志 VS 日志复制后3节点日志')
    put_row([table1, None, table2], size = '50% 10px 50%')
    put_row([table0, None, table1, None, table2], size = '10% 10px 45% 10px 45%')


