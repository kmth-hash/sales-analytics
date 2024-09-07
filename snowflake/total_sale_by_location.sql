select sum(total_price) , store_location from 
(
select u.* , s.total_price from (select _id , sum(items_price*items_qty) as total_price from salesdata group by _id) s join userdata u on u._id=s._id 
) s2 
group by 2
;