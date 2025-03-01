import requests

"""
To see more informations about the api :
https://datahub.bordeaux-metropole.fr/explore/dataset/ci_vcub_p/information/
"""

def load_json_bordeaux():
    """
    Load reponse JSON  from API with Bordeaux stations current status
    Return a response JSON
    """

    base_url = 'https://datahub.bordeaux-metropole.fr'
    endpoint = '/api/explore/v2.1/catalog/datasets/ci_vcub_p/exports/json'
    params = {
        'lang':'fr',
        'timezone':'Europe/Berlin'
    }
    response = requests.get(base_url+endpoint, params=params)
    status_code = response.status_code
    print(f"📥 geting data from {base_url + endpoint}...")
    print(f'status code: {status_code}')
    return response.json()

def extract_json_bordeaux(json_response):
    """
    take JSON response for Bordeaux stations to extract needed fields
    returns a dic
    """
    station_dic = {
        'lon': [],
        'lat': [],
        "insee": [],
        "commune": [],
        "gml_id": [],
        "gid": [],
        "ident": [],
        "type": [],
        "nom": [],
        "etat": [],
        "nbplaces": [],
        "nbvelos": [],
        "nbelec": [],
        "nbclassiq": [],
        "cdate": [],
        "mdate": [],
        "code_commune": []
    }

    dic_keys = list(station_dic.keys())
    print(f'🛠️ extracting Bordeaux JSON data...')
    stations = json_response
    for station in stations:
        for key in dic_keys:
            # some key are directly accessible while others should be accessed inside another JSON
            if key in list(station.keys()):
                station_dic[key].append(station.get(key))
            else:
                sub_json = station.get('geo_point_2d')
                station_dic[key].append(sub_json.get(key))
    print(f'✅ Bordeaux stations JSON data extracted')
    return station_dic


if __name__ == "__main__":
    json_response = load_json_bordeaux()
    dic = extract_json_bordeaux(json_response)
    print(len(dic['lon']))