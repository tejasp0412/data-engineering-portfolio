# Marketplace Payment Platform — Analytics Pipeline

End-to-end analytics pipeline on the Olist Brazilian e-commerce dataset, reframed as a **Marketplace Payment Processing Platform**. Covers automated FX ingestion, dbt modelling, data quality checks, and a 4-page Looker Studio dashboard.

🔗 **[View Live Dashboard](https://lookerstudio.google.com/u/0/reporting/1360e164-0def-4086-bec1-4d9b6b360469/page/IjlpF)**

---

## Stack

| Tool | Role |
|---|---|
| Python | Automated BRL/USD FX rate ingestion |
| dbt + DuckDB | Data transformation and modelling |
| Looker Studio | Dashboard and reporting |

---

## Pipeline

```
Raw CSVs → Python (FX rates) → dbt Staging → Intermediate → Marts → Looker Studio
```

### DAG

![Lineage DAG](dag.png)

---

## Models

| Layer | Models |
|---|---|
| Staging | stg_orders, stg_customers, stg_payments, stg_order_items, stg_products, stg_brl_usd_exchange_rate |
| Intermediate | int_orders_enriched, int_customer_payment_history |
| Core Marts | fct_orders, fct_daily_revenue, dim_customers, dim_products |
| Finance Marts | fct_payment_operations, fct_customer_kpis |

All 12 KPIs are pre-computed in the modelling layer with documented formulas and competing stakeholder definitions. A shared KPI catalogue ensures every team is measuring the same thing the same way — eliminating ambiguity before it reaches a business decision. 🔗 [KPI Catalogue](https://lookerstudio.google.com/u/0/reporting/1360e164-0def-4086-bec1-4d9b6b360469/page/p_iakse9v70d)

---

## Data Quality

7 checks across 5 categories — each with a documented failure action.

| Check | On Failure |
|---|---|
| PK uniqueness on all mart tables | Block |
| Every transaction links to a known customer | Block |
| No payment without a matching order | Block |
| No zero/negative payment values | Block |
| Every delivered order has at least one line item | Block |
| Source freshness on raw orders (warn 24h, error 48h) | Alert |
| Daily revenue within 3 standard deviations | Warn only |

Anomaly detection is warn-only by design as a revenue spike may be a legitimate campaign or a targeted promotion, not bad data.

---

## Key Findings

- **Boleto has a 16.85% SLA breach rate** vs 1% for credit card — a structural cash flow risk affecting 20% of transactions
- **High Risk customers outspend Low Risk ones** ($61 vs $37 avg CLV) — installment rate alone is too blunt a risk signal
- **Top 10% of customers drive 38.6% of TPV** — revenue concentration worth actively protecting

---

## Quick Start

```bash
python3 -m venv venv && source venv/bin/activate
pip install dbt-duckdb pandas requests

python3 scripts/get_brl_usd_cnv_rates.py
dbt seed --full-refresh
dbt run --full-refresh --no-partial-parse
dbt test --no-partial-parse
python3 scripts/export_to_csv.py
```

---

## Design Decisions

**Ephemeral intermediates** — business logic lives in one place. Change `is_delivered` once and all downstream models pick it up.

**KPIs in dbt, not the dashboard** — one definition, one formula, one place to update. No diverging numbers across teams.

**DuckDB locally, cloud-ready** — swapping to Snowflake or BigQuery is a one-line change in `profiles.yml`.