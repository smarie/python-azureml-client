from azmlclient import ClientConfig, GlobalConfig, ServiceConfig


def test_empty_cfg():
    """Tests that a minimum manually created configuration dumps and loads without issue"""

    cfg = ClientConfig()
    s = cfg.dumps_yaml(default_flow_style=False)
    assert s == """!yamlable/org.pypi.azmlclient.ClientConfig
global:
  http_proxy: null
  https_proxy: null
  ssl_verify: true
services: {}
"""
    cfg2 = ClientConfig.loads_yaml(s)
    assert cfg == cfg2


def test_full_cfg():
    """Tests that a manually created configuration dumps and loads without issue"""

    global_cfg = GlobalConfig(https_proxy="http://localhost:8888")
    first_service_cfg = ServiceConfig(base_url="https://blah", api_key="hiojdkfml")
    second_service_cfg = ServiceConfig(base_url="https://blah2", api_key="hiojdkfml2")
    cfg = ClientConfig(global_cfg, first_service=first_service_cfg, second_service=second_service_cfg)

    s = cfg.dumps_yaml(default_flow_style=False)
    assert s == """!yamlable/org.pypi.azmlclient.ClientConfig
global:
  http_proxy: null
  https_proxy: http://localhost:8888
  ssl_verify: true
services:
  first_service:
    api_key: hiojdkfml
    base_url: https://blah
    blob_account: null
    blob_api_key: null
    blob_container: null
    blob_path_prefix: null
    by_ref_api_key: hiojdkfml
    by_ref_base_url: https://blah
  second_service:
    api_key: hiojdkfml2
    base_url: https://blah2
    blob_account: null
    blob_api_key: null
    blob_container: null
    blob_path_prefix: null
    by_ref_api_key: hiojdkfml2
    by_ref_base_url: https://blah2
"""
    cfg2 = ClientConfig.loads_yaml(s)
    assert cfg == cfg2
