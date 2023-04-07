import time
from pysyncobj import SyncObj, replicated
from pysyncobj.config import SyncObjConf

class SyncPoolObject(SyncObj):
    def __init__(self, self_node, other_nodes, **kwargs):
        SyncObj.__init__(self, self_node, other_nodes, **kwargs)
        self.__collect_agent_version = ''  # collect_agent版本号

    @replicated
    def set_collect_agent_version(self, version):
        self.__collect_agent_version = version

    def get_collect_agent_version(self):
        return self.__collect_agent_version

conf = SyncObjConf(dynamicMembershipChange=True)
sync_pool1 = SyncPoolObject('127.0.0.1:4321', ['127.0.0.1:4322', '127.0.0.1:4323'], conf=conf)
sync_pool2 = SyncPoolObject('127.0.0.1:4322', ['127.0.0.1:4321', '127.0.0.1:4323'], conf=conf)
sync_pool3 = SyncPoolObject('127.0.0.1:4323', ['127.0.0.1:4321', '127.0.0.1:4322'], conf=conf)

time.sleep(5)
print(sync_pool1.getStatus())
print(sync_pool2.getStatus())
print(sync_pool3.getStatus())
print(sync_pool2.get_collect_agent_version())
# 输出空值
time.sleep(2)
sync_pool1.set_collect_agent_version(5)
time.sleep(2)
print(sync_pool1.get_collect_agent_version())
# 5
print(sync_pool2.get_collect_agent_version())
# 输出5
print(sync_pool3.get_collect_agent_version())
# 输出5