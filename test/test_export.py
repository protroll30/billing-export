############ DISCLAIMER ####################
# Copyright 2019 Google LLC. This software is provided as-is, without warranty or representation 
# for any use or purpose. Your use of it is subject to your agreement with Google. 
############################################

import sys

from src.export import *
from datetime import datetime

os.chdir('../../billing-export/')
sys.path.append(os.getcwd())
print(os.getcwd())


def test_parse_args(opts):
    if opts.config_file is None:
        assert("config file is not provided")

    if opts.export_start_date is not None:
        assert (opts.export_start_date == datetime.strptime(opts.export_start_date, "%Y%m%d").strftime('%Y%m%d'))
    else:
        print("export_start_date is not provided")

    if opts.export_end_date is not None:
        assert (opts.export_end_date == datetime.strptime(opts.export_end_date, "%Y%m%d").strftime('%Y%m%d'))
    else:
        print("export_end_date is not provided")

    if opts.historical_run is not None:
        assert (opts.historical_run == bool)
    else:
        print("historical_run is not provided")


def test_read_exporter_config(config, opts):

    config_data = read_exporter_config(opts)
    assert config_data['destination_bucket'] is not None
    assert config_data['destination_bucket'] == config.bucket_name
    assert config_data['source_dataset_id'] == config.dataset_id
    assert config_data['source_dataset_id'] is not None
    assert config_data['source_project_id'] == config.project
    assert config_data['source_project_id'] is not None
    assert config_data['source_table_id'] == config.table_id
    assert config_data['source_table_id'] is not None

def test_config(opts):
    exporter_config = read_exporter_config(opts)
    config_data = Config(exporter_config)

    assert logger.hasHandlers()
    assert config_data.big_query_client is not None
    assert config_data.storage_client is not None
    assert config_data.bucket_name is not None
    assert config_data.gcs_bucket is not None

    assert config_data.extract_status_file is not None
    assert config_data.gcs_extract_status_file_blob is not None
    assert config_data.local_extract_status_file_path is not None

    assert config_data.process_status_folder is not None
    assert config_data.gcs_process_status_folder_blob is not None

    assert config_data.project is not None
    assert config_data.dataset_id is not None
    assert config_data.table_id is not None

    assert config_data.logger_name is not None


def test_lookup_extract_bucket(config_data):
    assert lookup_extract_bucket(config_data) is not None


def test_extract_status_file_exists(config_data):
    assert extract_status_file_exists(config_data) is not None


def test_create_folder_in_bucket(config_data):
    """Provides a pre-existing blob in the test bucket."""
    test_folder = 'test/'
    blob = config_data.gcs_bucket.blob(test_folder)
    create_folder_in_bucket(blob)
    print("folder created ...")

    # Clean up
    blob.delete()
    print("folder deleted ...")

def test_create_local_extract_status_file():
    test_filename = 'process_status/test_extract_status_file'
    create_local_extract_status_file(test_filename)
    print("test_extract_status_file created ...")

    # Clean up
    os.remove(test_filename)
    print("test_extract_status_file deleted ...")


def test_get_latest_extract_date_from_statusfile(config_data):
    date_string = get_latest_extract_date_from_statusfile(config_data)
    date_format = '%Y%m%d'
    date_obj = datetime.strptime(date_string, date_format)
    print("date_string is {}".format(date_string))
    assert date_obj == datetime.strptime(date_string, date_format)


def test_get_partitions(config_data, opts):
    assert get_partitions(config_data, opts.export_start_date, opts.export_end_date) is not None


def test_get_dataset_ref(config_data):
    assert get_dataset_ref(config_data) is not None

def test_write_to_local_status_file():
    test_file_name = 'process_status/test_extract_status_file'
    data = set_extract_status_json_data()
    write_to_local_status_file(test_file_name,data)

    # Clean up
    os.remove(test_file_name)
    print("test_extract_status_file deleted ...")


def test_upload_file_to_gcs(config_data):
    test_file = "process_status/test.log"
    blob = config_data.gcs_bucket.blob(test_file)
    upload_file_to_gcs(blob, config_data.log_file)
    print("log file created ...")

    # Clean up
    blob.delete()
    print("log file deleted ...")
