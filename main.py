import requests
from bs4 import BeautifulSoup as bs 
import json
import re
import math
from tqdm import tqdm
import concurrent.futures

def get_initial_data(url):
    headers = {'user-agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'}
    res = requests.get(url, headers=headers)
    soup = bs(res.content, features="html.parser")
    data = soup.find('script', {"id":"initial-data"}).attrs.get('data-json') 
    return json.loads(data)

def get_locations(url):
    data = get_initial_data(url)
    locations = data['listingProps']['nextLocations'][0]['locations']
    for location in locations:
        location['name'] = location['label']
        location['url'] = f"{url}?sd={location['value']}"
        location['pages'] = math.ceil(location['count']/50)
        for key in ['value', 'label', 'level']:
            del location[key]
    return locations

def get_ad_list(url):
    data = get_initial_data(url)
    ad_list = data['listingProps']['adList']
    ad_list = [item.get('url') for item in ad_list]
    ad_list = list(filter(lambda item: item is not None, ad_list))
    return ad_list

def get_ad(url):
    data = get_initial_data(url).get('ad')
    ad = {}
    ad['id'] = data.get('adId')
    ad['url'] = data.get('friendlyUrl')
    ad['user'] = data.get('user',{}).get('name')
    ad['phone'] = data.get('phone',{}).get('phone')
    ad['price'] = data.get('price')
    ad.update( { item['name']:item['value'] for item in data['properties']  })
    ad.update( { item['label']:item['value'] for item in data['locationProperties']  })

    pattern = re.compile('.*_features')
    key_patterns = [key for key in ad.keys() if pattern.match(key)]
    ad['features'] = ', '.join([ad[key] for key in key_patterns])

    for key in key_patterns + ['real_estate_type']:
        del ad[key]

    return ad


data = []
url_start = 'https://pb.olx.com.br/paraiba/joao-pessoa/imoveis/venda'
locations = get_locations(url_start)
pbar = tqdm(locations)
for location in pbar:
    for page in range(1,location['pages']+1):
        url_location_page = f"{location['url']}&o={page}"
        ad_list = get_ad_list(url_location_page)
        with concurrent.futures.ProcessPoolExecutor(max_workers=100) as executor:
            result = executor.map(get_ad,ad_list)
            data.extend(result)
            pbar.set_description(f"{location['name']} - {page} de {location['pages']} ({len(data)})")

        
        
        # for url_ad in ad_list:
        #     pbar.set_description(f"{location['name']} - {page} de {location['pages']} ({len(data)})")
        #     ad = get_ad(url_ad)
        #     data.append(ad)


print(data)