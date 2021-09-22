n = 15
import random
table_prefix = 'table_FZJH_002'
_ = [i for i in range(2*n)];random.shuffle(_)
for i in range(n):
    field = ''.join(random.sample("zyxwvutsrqponmlkjihgfedcba",5))
    table_name = f'{table_prefix}_{i}'
    print(f'create table {table_name} (ID int, {field} char(10), primary key(ID))')
    print(f'insert into {table_name} values ({_[i]}, "{field}")')
for i in range(n):
    table_name = f'{table_prefix}_{i}'
    print(f'select * from {table_name}')
    print(f'drop table {table_name}')