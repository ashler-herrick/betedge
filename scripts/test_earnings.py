import requests

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


def main():
    url = "https://api.nasdaq.com/api/calendar/earnings"
    response = requests.get(url, headers=headers, params={"date": "2025-07-01"})
    rj = response.json()
    rjd = rj["data"]
    rjdr = rjd["rows"]
    for row in rjdr:
        print(row["time"])


if __name__ == "__main__":
    main()
