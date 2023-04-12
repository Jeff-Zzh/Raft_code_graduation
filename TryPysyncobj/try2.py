import time
from pysyncobj import SyncObj, SyncObjConf

conf = SyncObjConf(dynamicMembershipChange = True)  # 集群默认不允许动态添加节点，除非在创建时进行配置
sync_pool1 = SyncObj('127.0.0.1:4321', ['127.0.0.1:4322', '127.0.0.1:4323'], conf = conf)
sync_pool2 = SyncObj('127.0.0.1:4322', ['127.0.0.1:4321', '127.0.0.1:4323'], conf = conf)
sync_pool3 = SyncObj('127.0.0.1:4323', ['127.0.0.1:4321', '127.0.0.1:4322'], conf = conf)

time.sleep(5)
print(sync_pool1.getStatus())

sync_pool1.addNodeToCluster('127.0.0.1:4324')
sync_pool4 = SyncObj('127.0.0.1:4324', ['127.0.0.1:4321', '127.0.0.1:4322', '127.0.0.1:4323'], conf=conf)

time.sleep(10)  # 有时时间短，还连接不上sync_pool4
print(sync_pool1.getStatus())

# 没运行addNodeToCluster()，直接创建节点，没用
sync_pool5 = SyncObj('127.0.0.1:4325', ['127.0.0.1:4321', '127.0.0.1:4322', '127.0.0.1:4323', '127.0.0.1:4324'], conf=conf)
time.sleep(5)
print(sync_pool5.getStatus())