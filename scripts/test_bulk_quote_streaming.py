import httpx  # install via pip install httpx

BASE_URL = "http://127.0.0.1:25510/v2"  # all endpoints use this URL base

# set params
params = {
  'root': 'AAPL',
  'exp': '20250117',
  'start_date': '20241107',
  'end_date': '20241107',
}

#
# This is the streaming version, and will read line-by-line
#
url = BASE_URL + '/bulk_hist/option/quote'

while url is not None:
    with httpx.stream("GET", url, params=params, timeout=60) as response:
        response.raise_for_status()  # make sure the request worked
        for line in response.iter_lines():
            print(line)  # do something with the data

    # check the Next-Page header to see if we have more data
    if 'Next-Page' in response.headers and response.headers['Next-Page'] != "null":
        url = response.headers['Next-Page']
        params = None
    else:
        url = None
