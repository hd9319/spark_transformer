import os
import sys
import json

from .components.context import create_config, create_context
from .components.transformers import read_csv, inspect_data, transform, download_health_summary

if __name__ == '__main__':
    # Define Global Variables
    with open('config.json', 'r') as readfile:
        config = json.load(readfile)
        globals().update(config)

    # Initialize Context
    sc_config = create_config(master_url=master_url, app_name=app_name, memory=memory)
    sc, sql_context = create_context(sc_config)

    try:   
        # Read Data
        args = {'inferSchema': True, 'header': True, 'encoding':'utf-8'}
        data = read_csv(file_path, **args)

        # Inspect Data
        _ = inspect_data(data)

        # Transform Data
        data, categorical_mappings = transform(data)

        # Download Aggregate Summary
        _ = download_health_summary(data, output_directory)

        # Close Spark
        sc.stop()

        print('Complete')

    except Exception as e:
        print(e)
        sc.stop()    