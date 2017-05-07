--create table mytable(id int)

--create table #table(id int)

--declare @n nvarchar(1);
--Declare @tabl table(id int)

--insert #table
--select *,1, t.ta,@n,(select * from t) from tabl1 t

--select @n
--with s as(
--	select * from ttt
--),s as(
--	select * from ttt
--)
select t.id,t1.namename
from --@tabl t 
	tabl t	
--join finance.tetra t1 on t.id = t1.parid and t1.name=t.part and t.i = t1.ii and @n = t1.ii11
where t.par is null

--t.id Equals t1.parid
--t1.name Equals t.part
--t.i Equals t1.ii