import pandas as pd
import requests
from collections import ChainMap
from functools import reduce

file_path = 'wa_or_zipcode.xlsx'

# Read files
df = pd.read_excel(file_path)
# Convert the columns to integer type

# Access the zip code columns 
or_column = df['OR'].dropna().reset_index(drop=True).astype(int).tolist()
wa_column = df['WA'].dropna().reset_index(drop=True).astype(int).tolist()

def get_uniqu_dealers(dealer_response_list):
    # cover the list dealer to the directionary 
    dealers_dict = {
        dealer['dealer']['id']: {
            'name': dealer['dealer']['name'],
            'phoneNumber': dealer['dealer']['phoneNumber'],
            'siteUrl': dealer['dealer']['siteUrl']
        }
        for dealer in dealer_response_list
    }
    return dealers_dict

def get_dealers(zipcode, count):
  """
    1.request the dealer by zipcode
    2. cover the dealer to directionary
  """
    url = f"https://www.subaru.com/services/dealers/distances/by/zipcode?zipcode={zipcode}&count={count}&type=Active"
    print("url=", url)
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        unique_dealer = get_uniqu_dealers(data)
        return unique_dealer
    else:
        print(f"Error fetching data. Status code: {response.status_code}-zipcode{zipcode}")
        return None

dealer_list_res = []
for zip in or_column:
    dealer_list_res.append(get_dealers(zip, 10))
    
merged_dict = reduce(lambda a, b: {**a, **b}, dealer_list_res)

def make_dealler_dictoexcel(dealer_dic, file_address):
    # convert directionary to excel
    data_list = [{'key': key, **values} for key, values in dealer_dic.items()]
    df = pd.DataFrame(data_list)
    df.to_excel(f'{file_address}.xlsx', index=False, engine='openpyxl')
    print('make it successfully')
    
    
async def get_dealers_cocurent(session, zipcode, count):
    url = f"https://www.subaru.com/services/dealers/distances/by/zipcode?zipcode={zipcode}&count={count}&type=Active"
    
    async with session.get(url) as response:
        if response.status == 200:
            data = await response.json()
            unique_dealer = get_uniqu_dealers(data)
            return unique_dealer
        else:
            print(f"Error fetching data. Status code: {response.status_code}-zipcode{zipcode}")
            return None

async def fetch_all_dealers(zip_codes, count):
     async with aiohttp.ClientSession() as session:
        tasks = []
        for zip_code in zip_codes:
            task= get_dealers_cocurent(session, zip_code, count)
            tasks.append(task)
        results = await asyncio.gather(*tasks)
        return results

# Run the asynchronous function
def run_event_loop(zip_codes, count):
    loop = asyncio.get_event_loop()
    combined_results = loop.run_until_complete(fetch_all_dealers(zip_codes, count))
    print("finish")
    return combined_results