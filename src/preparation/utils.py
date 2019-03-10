import yaml

def load_config(config_path):
    """Loads YAML config"""
    with open(config_path) as f:
        return yaml.load(f)