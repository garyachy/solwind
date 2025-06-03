def load_config():
    import json

    with open('config.json', 'r') as config_file:
        config = json.load(config_file)
    
    return config