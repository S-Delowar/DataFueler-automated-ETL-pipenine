import yaml


def load_schema(schema_filepath):
    with open("config/schema.yml", 'r') as file:
        schema = yaml.safe_load(file)
    return schema