import os
import requests


def generate_by_user_password(keycloak_addr, username, password):
    url = f"https://{keycloak_addr}/realms/UA/protocol/openid-connect/token"
    payload = {
        "username": username,
        "password": password,
        "grant_type": "password",
        "client_id": "ua-grant"
    }

    response = requests.post(url, data=payload, verify=False)

    if response.ok:
        response_json = response.json()
        return response_json.get("access_token")
    else:
        raise Exception(f"Could not get a token.: {response}")
