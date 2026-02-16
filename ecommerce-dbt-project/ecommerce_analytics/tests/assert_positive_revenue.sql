-- Revenue should never be negative

select order_date, gross_revenue
from {{ ref('fct_daily_revenue') }}
where gross_revenue < 0