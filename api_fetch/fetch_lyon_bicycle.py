import requests

"""
To see more informations about the api :
https://data.grandlyon.com/portail/fr/jeux-de-donnees/stations-velo-v-metropole-lyon-disponibilites-temps-reel/info
"""

def load_json_lyon():
    """
    Load reponse JSON  from API with Lyon stations current status
    Return a response JSON
    """
    base_url = "https://data.grandlyon.com"
    endpoint = "/fr/datapusher/ws/rdata/jcd_jcdecaux.jcdvelov/all.json"
    #here we use as parameters -> "maxfeatures": -1 and "start": 1 to get all the records
    params = {
        "maxfeatures": -1,
        "start": 1,
        "filename": "stations-velo-v-metropole-lyon-disponibilites-temps-reel"
    }

    response = requests.get(base_url+endpoint, params=params)
    status_code = response.status_code
    print(f"📥 geting data from {base_url + endpoint}...")
    print(f'status code: {status_code}')
    return response.json()

def extract_json_lyon(json_response):
    """
    take JSON response  for Lyon stations to extract needed fields
    returns a dic
    """
    station_dic = {
        'address': [],
        "address2": [],
        "address_jcd": [],
        "availability": [],
        "availabilitycode": [],
        "available_bike_stands": [],
        "available_bikes": [],
        "banking": [],
        "bike_stands": [],
        "bonus": [],
        "code_insee": [],
        "commune": [],
        "gid": [],
        "last_update": [],
        "last_update_fme": [],
        "last_update_gl": [],
        "lat": [],
        "lng": [],
        "bikes": [],
        "electricalBikes": [],
        "electricalInternalBatteryBikes": [],
        "electricalRemovableBatteryBikes": [],
        "mechanicalBikes": [],
        "stands": []
    }

    dic_keys = list(station_dic.keys())

    print(f'🛠️ extracting Lyon JSON data...')
    stations = json_response['values']
    for station in stations:
        for key in dic_keys:
            # some key are directly accessible while others should be accessed inside another JSON
            if key in list(station.keys()):
                station_dic[key].append(station.get(key))
            else:
                sub_json = station.get('main_stands')
                if key not in list(sub_json.keys()):
                    sub_json = sub_json.get('availabilities')
                station_dic[key].append(sub_json.get(key))

    print(f'✅ Lyon stations JSON data extracted')
    return station_dic

if __name__ == "__main__":
    response_json= load_json_lyon()
    lyon_dic = extract_json_lyon(response_json)
    print(len(lyon_dic['stands']))

