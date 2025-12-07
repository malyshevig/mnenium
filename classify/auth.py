import requests


def get_token():
    url = "https://ngw.devices.sberbank.ru:9443/api/v2/oauth"
    client_secret = "MDE5YTkyYTMtZGJhYi03NTNhLTllMzUtODY2Yjg4YTQ5NjZhOjk2ZDdiNDU1LWE2YTQtNDJiOS1hMmZhLTFlMGE5ZTc0YWMyMQ=="

    payload={
        'scope': 'GIGACHAT_API_PERS'
    }
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json',
        'RqUID': '210c8f96-0c1b-4550-8d4c-434b5e83994f',
        'Authorization': f'Basic {client_secret}'
    }

    response = requests.request("POST", url, headers=headers, data=payload, verify=False)

    return response.json()["access_token"]


def get_token_salut():
    url = "https://ngw.devices.sberbank.ru:9443/api/v2/oauth"
    client_secret = "MDE5YWNlYzgtYjc4Ni03ZDZiLWI1YTMtYTY0OGQ5MmZiNDJjOmUzMDgzM2EwLTgzMWItNGQ1OC05ZDdiLTQxYjQyMmI3NTE2Mg=="

    payload={
        'scope': 'SALUTE_SPEECH_PERS'
    }
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json',
        'RqUID': '210c8f96-0c1b-4550-8d4c-434b5e83994f',
        'Authorization': f'Basic {client_secret}'
    }

    response = requests.request("POST", url, headers=headers, data=payload, verify=False)

    return response.json()["access_token"]
