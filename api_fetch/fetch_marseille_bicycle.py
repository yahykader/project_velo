import requests

"""
To see more informations about the api :
 https://data.ampmetropole.fr/explore/dataset/gbfs-extract-station-information/information/?disjunctive.nom_division
"""

def load_json_marseille():
    """
    Load reponse JSON  from API with Marseille stations current status
    Return a response JSON
    """

    base_url = 'https://data.ampmetropole.fr'
    endpoint = '/api/explore/v2.1/catalog/datasets/gbfs-extract-station-information/exports/json'
    params = {
        'lang':'fr',
        'timezone':'Europe/Berlin'
    }

    response = requests.get(base_url+endpoint, params=params)
    status_code = response.status_code
    print(f"📥 geting data from {base_url + endpoint}...")
    print(f'status code: {status_code}')
    return response.json()

def extract_json_marseille(json_response):
    """
    take JSON response for Marseille stations to extract needed fields
    returns a dic
    """

    station_dic = {
        "station_id": [],
        "nom_division": [],
        "name": [],
        "capacity": [],
        "is_valet_station": [],
        "num_bikes_available": [],
        "num_docks_available": [],
        "is_installed": [],
        "is_renting": [],
        "is_returning": [],
        "lon": [],
        "lat": [],
        "last_reported_tr": [],
        "is_virtual_station": [],
        "message_velo": [],
        "message_dock_dispo": []
    }

    dic_keys = list(station_dic.keys())
    print(f'🛠️ extracting Marseille JSON data...')
    stations = json_response
    for station in stations:
        for key in dic_keys:
            # some key are directly accessible while others should be accessed inside another JSON
            if key in list(station.keys()):
                station_dic[key].append(station.get(key))
            else:
                sub_json = station.get('point_geo')
                station_dic[key].append(sub_json.get(key))
    print(f'✅ Marseille stations JSON data extracted')
    return station_dic

if __name__ == "__main__":
    json_response = load_json_marseille()
    dic= extract_json_marseille(json_response)
    print(len(dic['lon']))