-- 测试Client与Master的通信
-- 测试Master与miniSQL的通信
-- 测试Master对create方法的处理
create table table_SQLC_001 (ID int, name char(10), primary key(ID))
-- 测试Master对insert方法的处理
insert into table_SQLC_001 values (2, "nmsl")
-- 测试Client与RegionServer的通信
-- 测试RegionServer与miniSQL的通信
select * from table_SQLC_001
-- 测试Master对drop方法的处理
drop table table_SQLC_001
-- 测试Client端的缓存
-- 测试Client端表不存在时的处理
select * from table_SQLC_001