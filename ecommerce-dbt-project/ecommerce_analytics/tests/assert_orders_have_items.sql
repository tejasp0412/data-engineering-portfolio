-- Delivered Orders should have at least 1 item

select order_id
from {{ ref('fct_orders') }}
where is_delivered = true
and total_items = 0