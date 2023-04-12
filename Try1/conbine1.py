import sqlite3
from pysyncobj import SyncObj

class SyncedSqliteDatabase(SyncObj):
    def __init__(self, selfNodeAddr, partnerNodeAddrs):
        super(SyncedSqliteDatabase, self).__init__(selfNodeAddr, partnerNodeAddrs)  # pysyncobj初始化节点
        self.conn = sqlite3.connect('../DB/test.db')  #
    def set(self, key, value):
        cursor = self.conn.cursor()
        cursor.execute("INSERT INTO mytable (key, value) VALUES (?, ?)", (key, value))
        self.conn.commit()
    def get(self, key):
        cursor = self.conn.cursor()
        cursor.execute("SELECT value FROM mytable WHERE key = ?", (key,))
        row = cursor.fetchone()
        if row is not None:
            return row[0]
        else:
            return None
    def sync(self, other):
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM mytable")
        rows = cursor.fetchall()
        for row in rows:
            key = row[0]
            value = row[1]
            other.set(key, value)
    def close(self):  # 关闭本节点与数据库的连接
        self.conn.close()


if __name__ == '__main__':
    node1 = SyncedSqliteDatabase('localhost:4321', ['localhost:4322', 'localhost:4323'])
    node2 = SyncedSqliteDatabase('localhost:4322', ['localhost:4321', 'localhost:4323'])
    node3 = SyncedSqliteDatabase('localhost:4323', ['localhost:4321', 'localhost:4322'])

    node1.set('key1', 'value1')
    node2.set('key2', 'value2')
    node3.set('key3', 'value3')

    print(node1.get('key2'))
    print(node2.get('key3'))
    print(node3.get('key1'))