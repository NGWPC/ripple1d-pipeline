import yaml
import os

def load_ripple_settings(file_path):
    try:
        with open(file_path, 'r') as file:
            ripple_settings = yaml.safe_load(file)
        return ripple_settings
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        return None
    except yaml.YAMLError:
        print("Error: Invalid YAML format in the file.")
        return None

def print_ripple_values(settings):
    ripple_settings = settings["ripple_settings"]
    if ripple_settings:
        print(f"RAS_VERSION: {ripple_settings['RAS_VERSION']}")
        print(f"DEPTH_INCREMENT: {ripple_settings['DEPTH_INCREMENT']}")
        print(f"RESOLUTION: {ripple_settings['RESOLUTION']}")

def print_payload_values(settings):
    payload_settings = settings["payload_templates"]
    if payload_settings:
        #Examples of accessing nested data in yaml file
        print(f"payload_settings['conflate_model']['source_model_directory']: \n {payload_settings['conflate_model']['source_model_directory']}")
        print(f"payload_settings['create_fim_lib']['plans']: \n {payload_settings['create_fim_lib']['plans']}") 
              
def main():
    file_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
    ripple_settings = load_ripple_settings(file_path)
    
    if ripple_settings:
        print_ripple_values(ripple_settings)
        print_payload_values(ripple_settings)

if __name__ == "__main__":
    main()