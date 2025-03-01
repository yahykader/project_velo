import requests

"""
To see more informations about the api :
https://www.velib-metropole.fr/donnees-open-data-gbfs-du-service-velib-metropole
"""

def load_json_paris(endpoint):
    """
    Load reponse JSON from Paris bicycles API  to get: 
    - station_information.json endpoint (mainly for location) 
    - station_status.json endpoint (for the current status of stations)
    Return a response JSON
    The 2 endpoints can be chosen in parameters ()'location' or 'status')
    """
    base_url = 'https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/'
    if endpoint == 'location':
        full_endpoint = 'station_information.json'
    elif endpoint == 'status':
        full_endpoint = 'station_status.json'
    else:
        print("❌ you must choose a location parameter 'location' or 'status'")
    print(f"📥 geting data from {base_url + full_endpoint}...")   
    response = requests.get(base_url + full_endpoint)
    status_code = response.status_code     
    print(f"status code: {status_code}")
    return response.json()

def extract_json_paris(json_response, endpoint):
    """
    take JSON response  for Paris stations (with 'location' or 'status'  as parameter) to extract needed fields
    returns a dic
    """
    if endpoint == 'location':
        stations_dic = {
            'station_id': [],
            'name': [],
            'lat': [],
            'lon': []
        } 

    elif endpoint == 'status':
        stations_dic = {
            'station_id': [],
            'mechanical': [],
            'ebike': [],
            'numDocksAvailable': [],
            'is_installed': [],
            'is_renting': [],
            'is_returning': [],
            'last_reported': []
        }
    else:
        print("❌ you must choose a location parameter 'location' or 'status'")
    
    print(f'🛠️ extracting paris {endpoint} JSON data...')
    stations = json_response['data']['stations']
 
    for station in stations:
        for key in list(stations_dic.keys()):
            if key == 'mechanical':
                stations_dic[key].append(station['num_bikes_available_types'][0].get(key))
            elif key == 'ebike':
                stations_dic[key].append(station['num_bikes_available_types'][1].get(key))
            else: 
                stations_dic[key].append(station[key])
    print(f'✅ Paris stations {endpoint} JSON data extracted')
    return stations_dic




if __name__ == "__main__":
    location_response = load_json_paris('location')
    status_response = load_json_paris('status')
    location_dic = extract_json_paris(location_response, endpoint = 'location')
    status_dic = extract_json_paris(status_response, endpoint = 'status')
    print(len(status_dic['is_installed']))