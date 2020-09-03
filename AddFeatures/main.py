import requests
import arcpy
import json
import os

######################################
## TO BE MODIFIED BEFORE PUBLISHING ##
######################################
ROOT_PATH = 'C:\\Users\\jeff9123\\Documents\\github\\MESA-GP\\AddFeatures'
######################################
## TO BE MODIFIED BEFORE PUBLISHING ##
######################################

# CODE

def open_config():
  with open(os.path.join(ROOT_PATH, 'config.json')) as f:
    return json.load(f)

def get_portal_token():
  config = open_config()
  portal_props = config['portal']

  token = None
  portal_url = portal_props['url']

  if 'username' in portal_props and 'password' in portal_props:
    try:
      payload = {
        'username': portal_props['username'],
        'password': portal_props['password'],
        'referer': 'http://localhost',
        'client': 'referer',
        'f': 'json'
      }

      req = requests.post(portal_url + '/sharing/rest/generateToken', data=payload, verify=False)
      req_json = req.json()
      token = req_json['token']
    except (ValueError, KeyError):
      return None
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
  payload = {
    'f': 'json'
  }

  if token:
    payload['token'] = token
  
  payload['features'] = json.dumps(add_features_params)

  req = requests.post(service_url + '/addFeatures', data=payload, verify=False)
  req_json = req.json()

  if 'error' in req_json:
    if req_json['error']['code'] == 498:
      del payload['token']
      req = requests.post(service_url + '/addFeatures', data=payload, verify=False)
      req_json = req.json()
  return req_json

def main():
  in_add_features_params = arcpy.GetParameter(0) # in add features params
  in_add_features_params = in_add_features_params.replace('\\"', '"')
  try:
    in_add_features_params = json.loads(in_add_features_params)
  except:
    arcpy.SetParameter(2, 'Invalid addFeaturesParams JSON, try again')
    exit()

  in_service_url = arcpy.GetParameter(1)

  token = get_portal_token()
  add_features_resp = add_features(in_service_url, in_add_features_params, token)
  
  arcpy.SetParameter(2, json.dumps(add_features_resp))

if __name__ == '__main__':
  main()