-- age_group_view.sql

create or replace view age_group_view as 
select * , 
case 
    when user_age between 0 and 20 then '0-20' 
    when user_age between 21 and 30 then '21-30' 
    when user_age between 31 and 40 then '31-40' 
    when user_age between 41 and 50 then '41-50' 
    when user_age between 51 and 60 then '51-60'
    when user_age between 61 and 70 then '61-70'
    when user_age between 71 and 80 then '71-80'
    else 'above 80'
end as age_group
from userdata;

-- top 10 cutomers with highest sales

create view if not exists top10customers as select _id , sum(items_price*items_qty) as total_sales from salesdata group by _id order by 2 DESC limit 10 ;

