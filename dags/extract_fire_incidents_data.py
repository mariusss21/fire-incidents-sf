import requests
from datetime import datetime
import os
    
def extract_fire_incidents_parameters():
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Pragma': 'no-cache',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Sec-GPC': '1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="136", "Brave";v="136", "Not.A/Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"',
    }

    response = requests.get('https://data.sfgov.org/Public-Safety/Fire-Incidents/wr8u-xric/about_data', headers=headers)
    cacheBust = response.text.split('"rowsUpdatedAt":')[1].split(',')[0]
    date = datetime.now().strftime('%Y%m%d')

    return cacheBust, date

def download_fire_incidents_data(cacheBust, date):
    headers = {
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Pragma': 'no-cache',
        'Referer': 'https://data.sfgov.org/Public-Safety/Fire-Incidents/wr8u-xric/about_data',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-GPC': '1',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="136", "Brave";v="136", "Not.A/Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"',
    }

    params = {
        'fourfour': 'wr8u-xric',
        'cacheBust': cacheBust,
        'date': date,
        'accessType': 'DOWNLOAD',
    }

    response = requests.get('https://data.sfgov.org/api/views/wr8u-xric/rows.csv', params=params, headers=headers)

    filename = f'fire_incidents_{params["date"]}.csv'
    with open(filename, 'wb') as f:
        f.write(response.content)

    return filename

def delete_local_file(filename):
    os.remove(filename)


if __name__ == '__main__':
    cacheBust, date = extract_fire_incidents_parameters()
    filename = download_fire_incidents_data(cacheBust, date)
    delete_local_file(filename)