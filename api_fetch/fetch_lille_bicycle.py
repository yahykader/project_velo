import requests

"""
To see more informations about the api :
 https://data.lillemetropole.fr/catalogue/dataset/c8166e8f-36a2-40ca-af1a-a00ab1fb20f7
"""

def load_json_lille():
    """
    Load reponse JSON  from API with Lille stations current status
    Return a response JSON
    """
    base_url = 'https://data.lillemetropole.fr'
    endpoint = '/data/ogcapi/collections/ilevia:vlille_temps_reel/items'
    params = {
        'f': 'json',
        'limit': '-1'
    }

    response = requests.get(base_url+endpoint, params=params)
    status_code = response.status_code
    print(f"📥 geting data from {base_url + endpoint}...")
    print(f'status code: {status_code}')
    return response.json()

def extract_json_lille(json_response):
    """
    take JSON response for Lille stations to extract needed fields
    returns a dic
    """

    station_dic = {
        "@id": [],
        "nom": [],
        "adresse": [],
        "code_insee": [],
        "commune": [],
        "etat": [],
        "type": [],
        "nb_places_dispo": [],
        "nb_velos_dispo": [],
        "etat_connexion": [],
        "x": [],
        "y": [],
        "date_modification": []
    }

    dic_keys = list(station_dic.keys())
    print(f'🛠️ extracting Lille JSON data...')
    stations = json_response.get('records')
    for station in stations:
        for key in dic_keys:
            # some key are directly accessible while others should be accessed inside another JSON
            station_dic[key].append(station.get(key))
    

    print(f'✅ Lille stations JSON data extracted')
    
    station_dic['id'] = station_dic['@id']
    del station_dic['@id']
    return station_dic

if __name__ == "__main__":
    json_response = load_json_lille()
    dic = extract_json_lille(json_response)
    print(len(dic['x']))