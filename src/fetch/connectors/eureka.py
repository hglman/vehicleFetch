import socket

from py_eureka_client import eureka_client


def build_eureka_config(hostname, service_port, app_name):
    return {
        "hostname": hostname,
        "service_port": service_port,
        "app_name": app_name
    }


def register_eureka(config):
    return eureka_client.init_registry_client(
        eureka_server=config["hostname"],
        app_name=config["app_name"],
        instance_port=int(config["service_port"]))


def init_eureka_discovery(config):
    return eureka_client.init_discovery_client(
        f"http://{socket.gethostbyname(config['hostname'])}:{config['port']}/{config['slug']}"
    )


def get_eureka_discovery():
    return eureka_client.get_discovery_client()


def build_req_service_config(application_name, service_slug, http_method, header):
    return {
        "application_name": application_name,
        "service_slug": service_slug,
        "http_method": http_method,
        "header": header
    }


def req_service(config, body):
    return get_eureka_discovery().do_service(
        config["application_name"],
        config["service_slug"],
        method=config["http_method"],
        data=bytes(body, "utf-8"),
        headers=config["header"],
        return_type="json",
        prefer_ip=True
    )
