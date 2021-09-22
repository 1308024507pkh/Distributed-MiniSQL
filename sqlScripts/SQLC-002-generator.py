print('create table table_SQLC_002 (ID int, name char(10), primary key(ID))')
n = 50
import random
_ = [i for i in range(2*n)];random.shuffle(_)
for i in range(n):
    name = ''.join(random.sample("zyxwvutsrqponmlkjihgfedcba",5))
    print(f'insert into table_SQLC_002 values ({_[i]}, "{name}")')
print(f'select * from table_SQLC_002 where ID >= {n-n//2} and ID <= {n+n//2}')
print(f'drop table table_SQLC_002')