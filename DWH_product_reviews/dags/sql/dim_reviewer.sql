insert into curated.dim_reviewer (reviewer_id,reviewer_name,start_date,end_date,ins_ts,upd_ts)
select distinct reviewerid,reviewername,'01-01-1900'::date,'01-01-2100'::date,now(),now() from staging.reviews
on conflict (reviewer_id, end_date)
do
update set end_date=now()::date , upd_ts=now() where curated.dim_reviewer.end_date='01-01-2100' and (curated.dim_reviewer.reviewer_name != excluded.reviewer_name);
 
insert into curated.dim_reviewer (reviewer_id,reviewer_name,start_date,end_date,ins_ts,upd_ts)
select distinct reviewerid,reviewername,now()::date,'01-01-2100'::date,now(),now() from staging.reviews
on conflict (reviewer_id, end_date)
do
update set end_date=now()::date , upd_ts=now() where curated.dim_reviewer.end_date='01-01-2100' and (curated.dim_reviewer.reviewer_name != excluded.reviewer_name);
 

--TO process the data intially we have to use below query (to choose only one name per reviewer_id-- 
/* 
insert into curated.dim_reviewer (reviewer_id,reviewer_name,start_date,end_date,ins_ts,upd_ts)
select reviewerid,reviewername,start_date,end_date,ins_ts,upd_ts from (
select reviewerid,reviewername,start_date,end_date,ins_ts,upd_ts,row_number() over(partition by reviewerid order by reviewername ) as rnk from 
(select distinct reviewerid,reviewername,'01-01-1900'::date start_date,'01-01-2100'::date end_date,now() ins_ts,now() upd_ts from staging.reviews) t1) t2 where rnk=1

on conflict (reviewer_id, end_date)
do
update set end_date=now()::date , upd_ts=now() where curated.dim_reviewer.end_date='01-01-2100' and (curated.dim_reviewer.reviewer_name != excluded.reviewer_name);
*/