import sqlite3
from Utils.utils import init_table_info
conn = sqlite3.connect('../DB/test.db')
table_info = init_table_info()
cur = conn.cursor()

