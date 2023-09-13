insert into curated.dim_product (asin,title,description,category,raw_categories,price,price_bucket_id,sales_rank,imageurl,brand,related,start_date,end_date,ins_ts,upd_ts)
select  distinct asin,title,description,
category,categories,price,price_bucket_id,salesrank,imurl,brand,related,
'01-01-1900'::date,'01-01-2100'::date,now(),now() from staging.products p left outer join curated.dim_price_bucket pb on p.price >=pb.lower_limit and p.price < upper_limit
on conflict (asin, end_date)
do
 update set end_date=now()::date , upd_ts=now() where curated.dim_product.end_date='01-01-2100' and (
 curated.dim_product.title != excluded.title or
 curated.dim_product.description != excluded.description or
 curated.dim_product.category != excluded.category or
 curated.dim_product.raw_categories != excluded.raw_categories or
 curated.dim_product.price != excluded.price or
 curated.dim_product.price_bucket_id != excluded.price_bucket_id or
 curated.dim_product.sales_rank != excluded.sales_rank or
 curated.dim_product.imageurl != excluded.imageurl or
 curated.dim_product.brand != excluded.brand or
 curated.dim_product.related != excluded.related)
 ;
 
 
 
 
insert into curated.dim_product (asin,title,description,category,raw_categories,price,price_bucket_id,sales_rank,imageurl,brand,related,start_date,end_date,ins_ts,upd_ts)
select  distinct asin,title,description,
category,categories,price,price_bucket_id,salesrank,imurl,brand,related,
now()::date,'01-01-2100'::date,now(),now() from staging.products p left outer join curated.dim_price_bucket pb on p.price >=pb.lower_limit and p.price < upper_limit
on conflict (asin, end_date)
do
 update set end_date=now()::date , upd_ts=now() where curated.dim_product.end_date='01-01-2100' and (
 curated.dim_product.title != excluded.title or
 curated.dim_product.description != excluded.description or
 curated.dim_product.category != excluded.category or
 curated.dim_product.raw_categories != excluded.raw_categories or
 curated.dim_product.price != excluded.price or
 curated.dim_product.price_bucket_id != excluded.price_bucket_id or
 curated.dim_product.sales_rank != excluded.sales_rank or
 curated.dim_product.imageurl != excluded.imageurl or
 curated.dim_product.brand != excluded.brand or
 curated.dim_product.related != excluded.related)
 ;