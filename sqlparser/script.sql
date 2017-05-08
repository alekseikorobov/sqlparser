--create table mytable(id int)

--create table #table(id int)

--declare @n nvarchar(1);
--Declare @tabl table(id int)

Declare @t1 bigint = 111111111111111111111111
Declare @t2 float = 5.0;

Declare @top int = 1;
Declare @t int = (select @top)
--Declare @t3 int = (select top(@top) 1 from t where ttt = 1)
--insert #table
--select *,1, t.ta,@n,(select * from t) from tabl1 t

--select @n
--with s as(
--	select * from ttt
--),s as(
--	select * from ttt
--)
--select t.id,t1.namename
--from --@tabl t 
--	t1 t 
--	--join t on t.id=t1.id
----join finance.tetra t1 on t.id = t1.parid and t1.name=t.part and t.i = t1.ii and @n = t1.ii11
--where t.par is null
--and t.name = t.reper;

--t.id Equals t1.parid
--t1.name Equals t.part
--t.i Equals t1.ii