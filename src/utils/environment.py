import time
import os
import consul
import json

def set_os_env(consul_host: str, consul_port: str, consul_token: str,env_file: str):
    os.environ.update({'CONSUL_HTTP_ADDR': f"{consul_host}:{consul_port}"})
    os.environ.update({'CONSUL_HTTP_TOKEN': consul_token})

    c = consul.Consul()
    config_entries = c.kv.get(key=env_file, recurse=True)
    data_bytes = config_entries[1][0]['Value']
    # Decodifica los bytes a una cadena
    data_string = data_bytes.decode('utf-8')
    # Crea un diccionario vacio para almacenar los pares clave-valor
    data_dict = {}
    # Itera por cada línea en la cadena
    for line in data_string.splitlines():
        # Divide la línea en clave y valor
        key, value = line.split('=')
        # Agrega la clave y el valor al diccionario
        data_dict[key] = value
        os.environ.update({key: value})

    return data_dict

def get_sa_json_from_consul(consul_host: str, consul_port: str, consul_token: str, consul_sa_file: str) -> None:
    os.environ.update({'CONSUL_HTTP_ADDR': f"{consul_host}:{consul_port}"})
    os.environ.update({'CONSUL_HTTP_TOKEN': consul_token})

    c = consul.Consul()
    data = c.kv.get(key=consul_sa_file, recurse=True)
    data_bytes = data[1][0]['Value']
    data_str = data_bytes.decode("utf-8")
    data_json = json.loads(data_str)

    return data_json