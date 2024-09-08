create view if not exists sales_by_store as 
select store_location , sum(total_sales) as total_sales from userdata u join (
select sum(items_price*items_qty) as total_sales , _id  from salesdata group by _id
) as s on s._id=u._id group by store_location;