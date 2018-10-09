import requests
from common.setting import Proxy_Server


def proxy():
    return requests.get(Proxy_Server).content.decode("utf-8")
