print('create table table_SQLC_004 (ID int, name char(10), primary key(ID))')
n = 10
import random
_ = [i for i in range(2*n)];random.shuffle(_)
for i in range(n):
    name = ''.join(random.sample("zyxwvutsrqponmlkjihgfedcba",5))
    print(f'insert into table_SQLC_004 values ({_[i]}, "{name}")')
for i in range(n):
    lower = random.randint(0, n)
    upper = random.randint(n, 2*n)
    print(f'select * from table_SQLC_004 where ID >= {lower} and ID <= {upper}')
print(f'drop table table_SQLC_004')