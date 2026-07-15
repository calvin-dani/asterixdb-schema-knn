import os
import sys
import requests

ASTERIX_URL = "http://localhost:29002/query/service"
HEADERS = {"Content-Type": "application/x-www-form-urlencoded"}

def main():
    statement = """USE EXPERIMENT;
COPY Movie_10M_iq
FROM S3
PATH ("tmdb-data-10M")
WITH {
    "format": "json",
    "container": "sagemaker-generator",
    "region": "us-west-2",
    "accessKeyId": "<SET_AWS_ACCESS_KEY_ID>",
    "secretAccessKey": "<SET_AWS_SECRET_ACCESS_KEY>"
};"""

    data = {
        "statement": statement,
        "pretty": "true",
        "client_context_id": f"load_TMDB_Movie10M"
    }

    resp = requests.post(ASTERIX_URL, headers=HEADERS, data=data)
    try:
        resp.raise_for_status()
        js = resp.json()
        with open('load_log.txt', 'a', encoding='utf-8') as f:
            f.write(resp.text)
            f.write('\n')
    except requests.HTTPError as e:
        print("HTTP error from AsterixDB:", e)
        print("Response text:")
        print(resp.text)
        sys.exit(1)

    print("AsterixDB response:")
    print(resp.text)

if __name__ == "__main__":
    main()