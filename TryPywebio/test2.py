from pywebio.input import *
from pywebio.output import *

# 定义两个表格的数据
table1_data = [['A1', 'B1', 'C1'], ['A2', 'B2', 'C2'], ['A3', 'B3', 'C3']]
table2_data = [['D1', 'E1', 'F1'], ['D2', 'E2', 'F2'], ['D3', 'E3', 'F3']]

# 定义两个表格
table1 = put_table(table1_data, header=['Header1', 'Header2', 'Header3'])
table2 = put_table(table2_data, header=['Header4', 'Header5', 'Header6'])

# 将两个表格放在同一行上展示
put_row( [ put_table(table1_data, header=['Header1', 'Header2', 'Header3']), put_table(table2_data, header=['Header4', 'Header5', 'Header6'] )] )
table0 = put_table( [ [''], ['Node1'], ['Node2'], ['Node3'] ] )

put_row( [put_table([[''], ['Node1'], ['Node2'], ['Node3']]),  table1, table2] )
put_row( [table0, table1, table2] )
