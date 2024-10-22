create database if not exists target_db;

use target_db;

show tables;

select * from customers;

select * from geolocation;

select * from order_items;

select * from orders;

select * from payments;

select * from sellers;

select * from products;

select distinct customer_city 
from customers;

select count(order_id) 
from orders
where year(order_purchase_timestamp) = 2017;

select products.product_category as 'category',round(sum(payments.payment_value),2) as 'total_sales'
from order_items join payments
on payments.order_id = order_items.order_id
join products
on order_items.product_id = products.product_id
group by category;

select count(payments.order_id) * 100/(select count(payments.order_id) from payments) as 'percentage of installment orders'
from payments
where payments.payment_installments>0;

select customer_state,count(customer_id)
from customers
group by customer_state;

select monthname(order_purchase_timestamp) as 'month_name',count(order_id) as 'no_of_orders'
from orders
where year(order_purchase_timestamp) = 2018
group by month_name;

with cte as 
(select orders.order_id,orders.customer_id,count(order_items.order_item_id) as 'no_of_items'
from orders join order_items
on orders.order_id = order_items.order_id
group by orders.order_id,orders.customer_id)
select customers.customer_city,avg(cte.no_of_items) as 'avg_ppo'
from customers join cte
on customers.customer_id = cte.customer_id
group by customers.customer_city
order by avg_ppo desc
limit 10;

select products.product_category as 'category',round(round(sum(payments.payment_value),2) * 100/ (select round(sum(payments.payment_value),2) from payments),2) as 'percentage_of_total_sales'
from order_items join payments
on payments.order_id = order_items.order_id
join products
on order_items.product_id = products.product_id
group by category
order by percentage_of_total_sales desc;

select products.product_category,count(order_items.order_item_id) as 'no_of_items_sold',round(avg(order_items.price),2) as 'product_price'
from products join order_items
on products.product_id = order_items.product_id
group by products.product_category;

select sellers.seller_id, round(sum(payments.payment_value),2) as 'total_revenue'
from sellers join order_items
on sellers.seller_id = order_items.seller_id
join payments
on order_items.order_id = payments.order_id
group by sellers.seller_id
order by total_revenue desc;

select customer_id, order_purchase_timestamp, payment, avg(payment) over (partition by customer_id order by order_purchase_timestamp rows between unbounded preceding and current row) as 'moving_avg'
from 
(select orders.customer_id, orders.order_purchase_timestamp, payments.payment_value as 'payment'
from orders join payments
on orders.order_id = payments.order_id) as a;

select years,months,payments,round(sum(payments) over (order by years,months),2) as 'cumulative_sales'
from 
(select year(orders.order_purchase_timestamp) as 'years',month(orders.order_purchase_timestamp) as 'months', round(sum(payments.payment_value),2) as 'payments'
from orders join payments
on orders.order_id = payments.order_id
group by years,months
order by years,months) as a;

with cte as 
(select year(orders.order_purchase_timestamp) as 'years', round(sum(payments.payment_value),2) as 'payments'
from orders join payments
on orders.order_id = payments.order_id
group by years)
select years, round(((payments - lag(payments) over (order by years))/lag(payments) over (order by years)) * 100,2) as 'yoy_growth_rate'
from cte;

with cte1 as 
(select customers.customer_id,min(orders.order_purchase_timestamp) as 'first_order'
from customers join orders
on customers.customer_id = orders.customer_id
group by customers.customer_id), 
cte2 as 
(select cte1.customer_id,count(distinct orders.order_purchase_timestamp)
from cte1 join orders
on cte1.customer_id = orders.customer_id
and orders.order_purchase_timestamp > first_order
and orders.order_purchase_timestamp < date_add(first_order, interval 6 month)
group by cte1.customer_id)
select 100 * count(distinct cte1.customer_id)/count(distinct cte2.customer_id)
from cte1 left join cte2
on cte1.customer_id = cte2.customer_id;

select years, customer_id,payment,ranks
from (select year(orders.order_purchase_timestamp) as 'years', orders.customer_id,sum(payments.payment_value) as 'payment', dense_rank() over (partition by year(orders.order_purchase_timestamp) order by sum(payments.payment_value) desc) as 'ranks'
from orders join payments
on orders.order_id = payments.order_id
group by years,orders.customer_id) as a
where ranks<=3;