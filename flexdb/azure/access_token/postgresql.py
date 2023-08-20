import subprocess
import json

def get_access_token():
    cmd = "az account get-access-token --resource=https://ossrdbms-aad.database.windows.net"
    token_data = subprocess.check_output(cmd, shell=True).decode("utf-8")
    token = json.loads(token_data)["accessToken"]
    return token
