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