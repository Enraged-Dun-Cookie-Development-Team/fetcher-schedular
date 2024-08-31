import requests


def send_heartbeat():

    port = 12345
    headers = {'instance_id': 'lwt-01', 'instance_url': "http://127.0.0.1:8004"}
    url = 'http://0.0.0.0:{}/heartbeat'.format(port)
    res = requests.get(url, headers=headers)

    headers = {'instance_id': 'lwt-02', 'instance_url': "http://127.0.0.1:8005"}
    url = 'http://0.0.0.0:{}/heartbeat'.format(port)
    res = requests.get(url, headers=headers)