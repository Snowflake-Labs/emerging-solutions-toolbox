import pandas
import numpy as np
import json
import requests
import base64
import _snowflake
from _snowflake import vectorized
from requests.auth import HTTPBasicAuth
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

r_session = requests.Session()


def get_match_id(payload):


  key = _snowflake.get_generic_secret_string('my_cred');


  url = f"https://api.myptv.com/geocoding/v1/locations/by-address?state={payload['city']}&locality={payload['region']}&postalCode={payload['postal_code']}&street={payload['street_address']}&countryFilter={payload['iso_country_code']}"
  headers = {
        'apiKey': key
    }
  response = requests.request("GET", url, headers=headers)
  return json.loads(response.text)