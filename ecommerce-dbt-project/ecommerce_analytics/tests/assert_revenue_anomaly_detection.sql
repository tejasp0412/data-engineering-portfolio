-- Data Quality Check: Anomaly Detection
-- Flags any day where gross revenue deviates more than 3 std deviations from the mean
-- Failure action: ALERT only, do NOT block — spikes may be legitimate (e.g. promotions)
-- False positive handling: 
--   1. Excludes alerts for any known events such as holiday events - Thanksgiving, Black Fridat, Christmas, New Year, etc.
--   2. Review flagged dates manually before rerunning pipeline
--   3. If recurring pattern (e.g. month-end spike), adjust to rolling 30-day window

{{ config(severity='warn') }}

with daily_revenue as (
    select
        order_date,
        gross_revenue_usd
    from {{ ref('fct_daily_revenue') }}
    where order_date is not null
),

stats as (
    select
        avg(gross_revenue_usd)    as mean_revenue,
        stddev(gross_revenue_usd) as stddev_revenue
    from daily_revenue
),

anomalies as (
    select
        d.order_date,
        d.gross_revenue_usd,
        s.mean_revenue,
        s.stddev_revenue,
        round(
            abs(d.gross_revenue_usd - s.mean_revenue) 
            / nullif(s.stddev_revenue, 0)
        , 2)                      as z_score
    from daily_revenue d
    cross join stats s
)

select
    order_date,
    gross_revenue_usd,
    mean_revenue,
    stddev_revenue,
    z_score
from anomalies
where z_score > 3
-- Add filter to exclude any known holiday events