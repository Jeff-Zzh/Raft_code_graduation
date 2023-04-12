import time
import threading
from pysyncobj import SyncObj, SyncObjConf
from Utils.utils import in_out_log, log_folder



conf = SyncObjConf(dynamicMembershipChange = True)  # 集群默认不允许动态添加节点，除非在创建时进行配置
sync_pool1 = SyncObj('127.0.0.1:4321', ['127.0.0.1:4322', '127.0.0.1:4323'])
sync_pool2 = SyncObj('127.0.0.1:4322', ['127.0.0.1:4321', '127.0.0.1:4323'])
sync_pool3 = SyncObj('127.0.0.1:4323', ['127.0.0.1:4321', '127.0.0.1:4322'])


@in_out_log
def voting(log_name, sync_pool):
    ''' 投票选举主节点，日志记录

    主节点选取日志打印、记录
    :param log_name: 某node的日志文件名
    :param sync_pool: 某node
    :return:
    '''
    with open(log_folder + f'\{log_name}.txt', 'w+') as f:
        while(sync_pool.getStatus()['leader'] is None):
            node_state = sync_pool.getStatus()
            print(node_state, time.time())
            f.write(str(node_state) + '\n')  # 写日志
        # leader选举完毕
        print(sync_pool.getStatus())
        f.write(str(sync_pool.getStatus()) + '\n')
print('Leader Election')
t1 = threading.Thread(target=voting, args=('node1_log', sync_pool1))
t2 = threading.Thread(target=voting, args=('node2_log', sync_pool2))
t3 = threading.Thread(target=voting, args=('node3_log', sync_pool3))
t1.start()
t2.start()
t3.start()
t3.join(10)  # 3个子线程已开始执行，t3开启的最晚，所以等待t3执行完毕再解除阻塞，t3执行完毕前阻塞主线程

breakpoint()





time.sleep(5)
print(sync_pool1.getStatus())
# {'version': '0.3.10', 'revision': 'deprecated', 'self': TCPNode('127.0.0.1:4321'), 'state': 0, 'leader': TCPNode('127.0.0.1:4322'),
# 'partner_nodes_count': 2, 'partner_node_status_server_127.0.0.1:4323': 2, 'partner_node_status_server_127.0.0.1:4322': 2,
# 'readonly_nodes_count': 0, 'log_len': 2, 'last_applied': 2, 'commit_idx': 2, 'raft_term': 1, 'next_node_idx_count': 0,
# 'match_idx_count': 0, 'leader_commit_idx': 2, 'uptime': 5, 'self_code_version': 0, 'enabled_code_version': 0}
print(sync_pool2.getStatus())
print(sync_pool3.getStatus())

sync_pool3.destroy()  # node3断开连接
time.sleep(5)
print(sync_pool1.getStatus())  # 从node1看:node1成为leader，且与node3的链接断开
# {'version': '0.3.10', 'revision': 'deprecated', 'self': TCPNode('127.0.0.1:4321'), 'state': 2, 'leader': TCPNode('127.0.0.1:4321'),
# 'partner_nodes_count': 2, 'partner_node_status_server_127.0.0.1:4323': 0, 'partner_node_status_server_127.0.0.1:4322': 2,
# 'readonly_nodes_count': 0, 'log_len': 3, 'last_applied': 3, 'commit_idx': 3, 'raft_term': 3, 'next_node_idx_count': 2,
# 'next_node_idx_server_127.0.0.1:4323': 3, 'next_node_idx_server_127.0.0.1:4322': 4, 'match_idx_count': 2,
# 'match_idx_server_127.0.0.1:4323': 0, 'match_idx_server_127.0.0.1:4322': 3, 'leader_commit_idx': 3, 'uptime': 10,
# 'self_code_version': 0, 'enabled_code_version': 0}

sync_pool3 = SyncObj('127.0.0.1:4323', ['127.0.0.1:4321', '127.0.0.1:4322'])  # node3重新加入链接
time.sleep(5)
print(sync_pool1.getStatus())  # node1与node3链接又建立
# {'version': '0.3.10', 'revision': 'deprecated', 'self': TCPNode('127.0.0.1:4321'), 'state': 2, 'leader': TCPNode('127.0.0.1:4321'),
# 'partner_nodes_count': 2, 'partner_node_status_server_127.0.0.1:4323': 2, 'partner_node_status_server_127.0.0.1:4322': 2,
# 'readonly_nodes_count': 0, 'log_len': 3, 'last_applied': 3, 'commit_idx': 3, 'raft_term': 3, 'next_node_idx_count': 2,
# 'next_node_idx_server_127.0.0.1:4323': 4, 'next_node_idx_server_127.0.0.1:4322': 4, 'match_idx_count': 2, 'match_idx_server_127.0.0.1:4323': 3,
# 'match_idx_server_127.0.0.1:4322': 3, 'leader_commit_idx': 3, 'uptime': 15, 'self_code_version': 0, 'enabled_code_version': 0}