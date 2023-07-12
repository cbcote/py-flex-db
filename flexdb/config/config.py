import json
import yaml
import configparser
import os

def load_config(file_path):
    try:
        file_extension = os.path.splitext(file_path)[1]
        with open(file_path, 'r') as f:
            if file_extension == '.json':
                config = json.load(f)
            elif file_extension == '.yaml' or file_extension == '.yml':
                config = yaml.load(f)
            elif file_extension == '.ini':
                parser = configparser.ConfigParser()
                parser.read_file(f)
                config = {section: dict(parser.items(section)) for section in parser.sections()}
            else:
                raise Exception(f'Unsupported file type {file_extension}')
    
    except FileNotFoundError:
        raise Exception(f'File {file_path} not found')
    
    except (json.JSONDecodeError, yaml.YAMLError, configparser.Error) as e:
        raise Exception(f'Error parsing file {file_path}: {e}')
    
    # Load environment variables
    config.update({k: v for k, v in os.environ.items() if k.startswith('FLEXDB_')})
    
    return config