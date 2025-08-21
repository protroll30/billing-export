############ DISCLAIMER ####################
# Copyright 2019 Google LLC. This software is provided as-is, without warranty or representation 
# for any use or purpose. Your use of it is subject to your agreement with Google. 
############################################

import json
import os

import pytest

# set working directory
from src.export import read_exporter_config
from src.export import Config

if not ('/billing-export' in os.path.realpath(os.getcwd())):
    os.chdir("/opt/billing-export")


def pytest_addoption(parser):
    parser.addoption('--config_file', type=str, nargs='?', help='A required config json file')
    parser.addoption('--export_start_date', type=str, nargs='?',
                        help='An optional string for date with format yyyyMMdd')
    parser.addoption('--export_end_date', type=str, nargs='?',
                        help='An optional string for date with format yyyyMMdd')
    parser.addoption('--historical_run', type=bool,
                        help='An optional boolean value to run for historical export dates')
    parser.addoption("--cmdopt", action="store", default="type1",
        help="my option: type1 or type2")

@pytest.fixture(autouse=True)
def config(config_file):

    print("opts.config_file: {}".format(config_file))
    config_data = None

    try:
        config = config_file

        if config is not None:
            with open(config) as json_file:
                json_data = json.load(json_file)
                json_file.close()
                config_data = type("json_data",(object,),{"bucket_name":json_data['destination_bucket'],
                                            "project":json_data['source_project_id'],
                                            "dataset_id":json_data['source_dataset_id'],
                                            "table_id":json_data['source_table_id']})




        else:
            print("File not found")

    except :
        raise Exception("Please provide config file as argument of the script")

    return config_data


@pytest.fixture(scope='module')
def config_file(request):
    config_file = request.config.getoption("--config_file")
    return config_file


@pytest.fixture(scope='module')
def export_start_date(request):
    export_start_date=request.config.getoption("--export_start_date")
    return export_start_date


@pytest.fixture(scope='module')
def export_end_date(request):
    export_end_date=request.config.getoption("--export_end_date")
    return export_end_date


@pytest.fixture(scope='module')
def historical_run(request):
    historical_run = request.config.getoption("--historical_run")
    return historical_run


@pytest.fixture(scope='module')
def opts(config_file, export_start_date, export_end_date, historical_run):
    opts = type("myopts",(object,),{"config_file":config_file,
                                    "export_start_date":export_start_date,
                                    "export_end_date":export_end_date,
                                    "historical_run":historical_run})
    return opts


@pytest.fixture(autouse=True)
def config_data(opts):
    exporter_config = read_exporter_config(opts)
    config_data = Config(exporter_config)
    return config_data



