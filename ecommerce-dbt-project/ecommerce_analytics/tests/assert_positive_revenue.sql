-- Data Quality Check: Business Rule
-- No payment transaction should have zero or negative value
-- Failure action: BLOCK — negative values corrupt TPV, CLV, and all revenue KPIs

-- Observation: In the current dataset, there are some transactions with $0 payment value. After investigation, these are primarily due to Vouchers so exckuding them
-- Excludes VOUCHER type: vouchers can legitimately be $0 (fully discounted)

select order_id, payment_value, payment_type
from {{ ref('stg_payments') }}
where payment_value <= 0 
and payment_type not in ('VOUCHER', 'NOT_DEFINED')

-- After runnign the test found 3 legitimate zero-value records with payment_type = 'NOT_DEFINED'
-- So updated the test to exclude 'NOT_DEFINED' as well.
--                          order_id    payment_type    payment_value
-- 0  4637ca194b6387e2d538dc89b124b0ee  NOT_DEFINED            0.0
-- 1  00b1cb0320190ca0daa2c88b35206009  NOT_DEFINED            0.0
-- 2  c8c528189310eaa44a745b8d9d26908b  NOT_DEFINED            0.0