import requests
from datetime import datetime

headers = {
    "authority": "api.nasdaq.com",
    "accept": "application/json, text/plain, */*",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36",
    "origin": "https://www.nasdaq.com",
    "sec-fetch-site": "same-site",
    "sec-fetch-mode": "cors",
    "sec-fetch-dest": "empty",
    "referer": "https://www.nasdaq.com/",
    "accept-language": "en-US,en;q=0.9",
}


def __get_calendar_query(url: str, date: datetime = None, date_is_month: bool = False, paramsin=None):
    if paramsin is None:
        if date is None:
            if date_is_month:
                datestr = datetime.today().strftime("%Y-%m")
            else:
                datestr = datetime.today().strftime("%Y-%m-%d")
        else:
            if date_is_month:
                datestr = date.strftime("%Y-%m")
            else:
                datestr = date.strftime("%Y-%m-%d")

        params = {"date": datestr}
    else:
        params = paramsin

    response = requests.get(url, headers=headers, params=params)
    data = response.json()["data"]
    return data


def main():
    url = "https://api.nasdaq.com/api/calendar/earnings"
    response = requests.get(url, headers=headers, params={"date": "2025-06-27"})
    rj = response.json()
    rjd = rj["data"]
    rjdr = rjd["rows"]
    for row in rjdr:
        print(row["time"])


if __name__ == "__main__":
    main()
