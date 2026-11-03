import os
from os import path

import yaml


def load_yaml(yaml_file):
    with open(yaml_file, "r") as yamlfile:
        return yaml.load(yamlfile, Loader=yaml.SafeLoader)


def build_config(nonprod_yaml=None, prod_yaml=None, nocreds=False):
    """
    builds the config dictionary out of passed in yamls.
      :params
        - nonprod_yaml_path -- path to yaml with default values
        - prod_yaml_path -- path to yaml with production overrides
      :return
        config, a dictionary of values parsed from the yamls.
    """

    if nonprod_yaml is None and prod_yaml is None:
        nonprod_yaml = path.join(os.getcwd(), "doc_delete_job_config.yml")
        prod_yaml = path.join(os.getcwd(), "doc_delete_job_config_ext.yml")

    if nocreds:
        return load_yaml(nonprod_yaml)

    nonprod_dict = load_yaml(nonprod_yaml)

    return nonprod_dict


def get_config():
    config = build_config()
    return config
