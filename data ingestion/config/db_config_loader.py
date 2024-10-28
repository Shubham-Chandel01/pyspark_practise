import yaml

#loads mysql config from YAML
def load_db_config():
    with open('config/db_config.yaml', 'r') as file:
        return yaml.safe_load(file)['mysql']


def get_jdbc_connection():
    db_config = load_db_config()

    jdbc_url = f"jdbc:mysql://{db_config['host']}:{db_config['port']}/{db_config['database']}"

    return jdbc_url
