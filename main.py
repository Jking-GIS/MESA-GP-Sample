import requests
import arcpy
import json

# CONFIGURABLE PROPERTIES

portal_props = {
  'url': 'https://gis.eea.mass.gov/portal',
  'client_id': '',
  'client_secret': ''
}

# CODE

def get_portal_token():
  token = None
  portal_url = portal_props['url']

  if 'client_id' in portal_props and 'refresh_token' in portal_props:
    try:
      payload = {
        'client_id': portal_props['client_id'],
        'refresh_token': portal_props['refresh_token'],
        'grant_type': 'refresh_token'
      }

      req = requests.post(portal_url + '/sharing/rest/oauth2/token', data=payload, verify=False)
      req_json = req.json()
      token = req_json['access_token']
    except (ValueError, KeyError):
      return None
  elif 'client_id' in portal_props and 'client_secret' in portal_props:
    try:
      payload = {
        'client_id': portal_props['client_id'],
        'client_secret': portal_props['client_secret'],
        'grant_type': 'client_credentials'
      }

      req = requests.post(portal_url + '/sharing/rest/oauth2/token', data=payload, verify=False)
      req_json = req.json()
      token = req_json['access_token']
    except (ValueError, KeyError):
      return None

  return token

def add_features(service_url, add_features_params, token=None):
  if token:
    add_features_params['token'] = token
  
  add_features_params['features'] = json.dumps(add_features_params['features'])

  req = requests.post(service_url + '/addFeatures', data=add_features_params, verify=False)
  req_json = req.json()
  return req_json

def main():
  in_add_features_params = arcpy.GetParameter(0) # in add features params
  in_add_features_params = json.loads(in_add_features_params)

  in_service_url = arcpy.GetParameter(1)

  token = get_portal_token()
  add_features_resp = add_features(in_service_url, in_add_features_params, token)
  
  arcpy.SetParameter(2, json.dumps(add_features_resp))

if __name__ == '__main__':
  main()