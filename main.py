import asyncio
import pandas as pd
import os
import time
import httpx

current_time = int(time.time())

def fetch_aevo_assets():
    symbols = httpx.get("https://api.aevo.xyz/assets").json()
    return symbols

async def fetch_single_funding_rate(asset):
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(f'https://api.aevo.xyz/funding?instrument_name={asset}-PERP')
            data = response.json()
            funding_rate = data['funding_rate']

            return asset,funding_rate
        except Exception as e:
            
            return None

async def fetch_aevo_funding():
    try:
        assets = fetch_aevo_assets()
        funding_data = await asyncio.gather(*[fetch_single_funding_rate(asset) for asset in assets])
        
        # Filter out None values (assets for which fetching failed)
        funding_data = [item for item in funding_data if item is not None]

        # Convert the list of tuples into a DataFrame with specified column names
        df = pd.DataFrame(funding_data, columns=['Token Name', 'Funding Rate'])
        df['Protocol'] = 'Aevo'
        df['Funding Rate'] = df['Funding Rate'].astype(float)
        return df
    
    except Exception as e:
        print(f"An error occurred: {e}")


def fetch_mango_funding():
    try:
        data = httpx.get("https://api.mngo.cloud/data/v4/stats/perp-market-summary").json()
        extracted_rates = []
        for symbol, data_list in data.items():
            if data_list:  # Check if the list is not empty
                data_dict = data_list[0]  # Assuming there's only one dictionary per list
                funding_rate = data_dict.get("funding_rate")
                clean_symbol = symbol.replace("-PERP", "")  # Removing the -PERP suffix
                extracted_rates.append({
                    'Token Name': clean_symbol,
                    'Funding Rate': funding_rate
                })

        # Convert the list of dictionaries to a DataFrame
        df = pd.DataFrame(extracted_rates)
        df['Protocol'] = 'Mango'
        return(df)
    
    except Exception as e:
        print(f"An error occurred: {e}")


def fetch_vertex_funding(symbol = None):
    product_id_to_symbol = {
        2: "BTC",
        4: "ETH",
        6: "ARB",
        8:"BNB",
        10:"XRP",
        12:"SOL",
        14:"MATIC",
        16:"SUI",
        18:"OP",
        20:"APT",
        22:"LTC",
        24:"BCH",
        26:"COMP",
        28:"MKR",
        30:"PEPE",
        34:"DOGE",
        36:"LINK",
        38:"DYDX",
        40:"CRV"
    }

    json_data = {
            
            "funding_rates": {
                "product_ids": [2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 34, 36, 38, 40]
            
            }
        }
    try:

        data = httpx.post("https://prod.vertexprotocol-backend.com/indexer",json=json_data).json()

        converted_data = []

        for key, value in data.items():
            product_id = value['product_id']
            symbol = product_id_to_symbol.get(product_id)
            
            if symbol:  # Only process if the symbol exists in the map
                funding_rate = (int(value['funding_rate_x18']) / 10**18) / 24
                converted_data.append({
                    'Token Name': symbol,
                    'Funding Rate': funding_rate,
                })

        # Convert the list of dictionaries to a DataFrame
        df = pd.DataFrame(converted_data)
        df['Protocol'] = 'Vertex'
        return df
    except Exception as e:
        pass

def get_hl_funding():
    hl_url = "https://api.hyperliquid.xyz/info"
    hl_headers = {"Content-Type": "application/json"}
    data = {"type": "metaAndAssetCtxs"}
    req = httpx.post(url= hl_url, headers=hl_headers, json=data).json()
    # Extract 'ame values dynamically from the first item of the list in the response
    token_names = [item['name'] for item in req[0]['universe']]
    # Extract funding rates for each token in name
    new_dict = {
        'Token Name': token_names,
        'Funding Rate': [float(item['funding']) * 100 for item in req[1]],
        'Open Interest (in token)': [float(item['openInterest']) for item in req[1]]
    }
    rates_df = pd.DataFrame(new_dict).astype({"Token Name": "string", "Funding Rate": "float64", "Open Interest (in token)": "float64"})
    
    rates_df['Protocol'] = 'Hyperliquid'
    
    return rates_df


def fetch_and_save_hl_funding_snap( filename:str):
    funding_df = get_hl_funding()
    funding_df['timestamp'] = current_time

    if funding_df is not None:
        if os.path.exists(filename):
            existing_df = pd.read_csv(filename)
            combined_df = pd.concat([existing_df, funding_df], ignore_index=True)
            combined_df.to_csv(filename, index=False)
        else:
            funding_df.to_csv(filename, index=False)

def fetch_and_save_aevo_funding_snap( filename:str):
    funding_df = asyncio.run(fetch_aevo_funding())
    funding_df['timestamp'] = current_time

    if funding_df is not None:
        if os.path.exists(filename):
            existing_df = pd.read_csv(filename)
            combined_df = pd.concat([existing_df, funding_df], ignore_index=True)
            combined_df.to_csv(filename, index=False)
        else:
            funding_df.to_csv(filename, index=False)

def fetch_and_save_mango_funding_snap( filename:str):
    funding_df = fetch_mango_funding()
    funding_df['timestamp'] = current_time

    if funding_df is not None:
        if os.path.exists(filename):
            existing_df = pd.read_csv(filename)
            combined_df = pd.concat([existing_df, funding_df], ignore_index=True)
            combined_df.to_csv(filename, index=False)
        else:
            funding_df.to_csv(filename, index=False)

def fetch_and_save_vertex_funding_snap( filename:str):
    funding_df = fetch_vertex_funding()
    funding_df['timestamp'] = current_time

    if funding_df is not None:
        if os.path.exists(filename):
            existing_df = pd.read_csv(filename)
            combined_df = pd.concat([existing_df, funding_df], ignore_index=True)
            combined_df.to_csv(filename, index=False)
        else:
            funding_df.to_csv(filename, index=False)


def main():

    fetch_and_save_aevo_funding_snap(filename=f'aevo_funding_snap.csv')
    fetch_and_save_hl_funding_snap(filename=f'hl_funding_snap.csv')
    fetch_and_save_mango_funding_snap(filename=f'mango_funding_snap.csv')
    fetch_and_save_vertex_funding_snap(filename=f'vertex_funding_snap.csv')

if __name__ == "__main__":
    main()