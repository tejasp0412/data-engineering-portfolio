'''
import requests
import pandas as pd

url = "https://api.exchangerate-api.com/v4/latest/BRL"

try:
    response = requests.get(url)
    response.raise_for_status()  # Raise an error for HTTP errors
    data = response.json()

    # Extract the USD exchange rate
    usd_brl_rate = data['rates']['USD']
    usd_brl_rate_date = data['date']

    # Create a DataFrame to store the exchange rate and date
    exchange_rate_df = pd.DataFrame({
        'rate_date': [usd_brl_rate_date],
        'currency_code': ['BRL'],
        'exchange_rate_to_usd': [usd_brl_rate]
    })
    # Save the DataFrame to a CSV file
    output_path = 'seeds/raw_brl_usd_exchange_rate.csv'
    exchange_rate_df.to_csv(output_path, index=False)
    
except requests.RequestException as e:
    print(f"Error fetching exchange rates: {e}")
    exit(1)
'''
import requests
import pandas as pd

START_DATE = "2016-09-01"
END_DATE   = "2018-12-31"

url = f"https://api.frankfurter.app/{START_DATE}..{END_DATE}?from=BRL&to=USD"

try:
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    exchange_rate_df = pd.DataFrame([
        {'rate_date': date, 'currency_code': 'BRL', 'exchange_rate_to_usd': rates['USD']}
        for date, rates in data['rates'].items()
    ])

    # Forward-fill weekends/holidays so every calendar date has a rate
    # bfill handles leading nulls if start date falls before first business day
    exchange_rate_df['rate_date'] = pd.to_datetime(exchange_rate_df['rate_date'])
    exchange_rate_df = (
        exchange_rate_df
        .set_index('rate_date')
        .reindex(pd.date_range(START_DATE, END_DATE, freq='D'))
        .ffill()
        .bfill()
        .reset_index()
        .rename(columns={'index': 'rate_date'})
    )
    exchange_rate_df['rate_date'] = exchange_rate_df['rate_date'].dt.date
    exchange_rate_df['currency_code'] = 'BRL'
    exchange_rate_df = exchange_rate_df[['rate_date', 'currency_code', 'exchange_rate_to_usd']]

    output_path = 'seeds/raw_brl_usd_exchange_rate.csv'
    exchange_rate_df.to_csv(output_path, index=False)
    print(f"Saved {len(exchange_rate_df)} rows to {output_path}")

except requests.RequestException as e:
    print(f"Error fetching exchange rates: {e}")
    exit(1)

except requests.RequestException as e:
    print(f"Error fetching exchange rates: {e}")
    exit(1)