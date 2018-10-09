def MYSQL_conf(spider_name):
    return {
        "host": "47.94.165.62",
        "user": "wy",
        "port": 3306,
        "password": "52013145Wy@",
        "db": spider_name,
        "charset": 'utf8'
    }


MYSQL_TMP = MYSQL_conf("information_schema")


REDIS_Config = {
    'host': '47.94.165.62',
    'port': 8888,
    'password': '',
    'decode_responses': True
}

PROXYPOOL_SERVER = "HTTP://47.94.165.62:6101/get"
