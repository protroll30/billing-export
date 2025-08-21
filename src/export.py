# Copyright 2019 Google LLC. This software is provided as-is, without warranty or representation 
# for any use or purpose. Your use of it is subject to your agreement with Google. 
import os
import json
import argparse
import signal
import subprocess
import errno

import logging.config
# noinspection PyUnresolvedReferences
import sys

import google.cloud.logging

# noinspection PyUnresolvedReferences
from google.cloud import bigquery
# noinspection PyUnresolvedReferences
from google.cloud import storage
# noinspection PyUnresolvedReferences
from google.cloud.logging.handlers import CloudLoggingHandler

from datetime import datetime, timedelta
from json import JSONDecodeError
from pathlib import Path

extract_status_json_data = None

print("get working directory:: {}".format(os.getcwd()))

# Configure Logging
# Create the Logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger_formatter = logging.Formatter(fmt='%(asctime)s %(filename)s:%(lineno)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M')

# Create the Handler for console logging
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_log_formatter = logging.Formatter(fmt='%(filename)s:%(lineno)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M')
console_handler.setFormatter(console_log_formatter)
logger.addHandler(console_handler)

# Create the Handler for stackdriver
logger_client = google.cloud.logging.Client()
stackdriver_handler = CloudLoggingHandler(logger_client, name="billing-export")
cloud_logger = logging.getLogger('billing-export')
cloud_log_formatter = logging.Formatter(fmt='%(filename)s:%(lineno)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M')
stackdriver_handler.setFormatter(cloud_log_formatter)
logger.addHandler(stackdriver_handler)


class Config:

    # Check if config.ini exists and load/generate it
    def __init__(self, config):
        """
        Initialize a config file object
        """
        self.storage_client = storage.Client()
        self.big_query_client = bigquery.Client()

        self.bucket_name = config['destination_bucket']
        self.gcs_bucket = self.storage_client.get_bucket(self.bucket_name)

        self.project = config['source_project_id']
        self.dataset_id = config['source_dataset_id']
        self.table_id = config['source_table_id']

        logger.info("{} - config data passed : {}".format(self.table_id, config))


        try:

            if config['logger'] is not None:

                if "stackdriver" not in config['logger']:
                    logger.removeHandler(stackdriver_handler)
                    logger.info("{} - stackdriver handler removed ".format(self.table_id))

                if "console" not in config['logger']:
                    logger.removeHandler(console_handler)
                    logger.info("{} - console handler removed".format(self.table_id))

        except KeyError:
            logger.info("{} - logger attribute is not set in exporter-config. stackdriver, console loghandler are set.".format(self.table_id))


        # create logfile if does not exist
        log_file = "{}/process_status/exporter.log".format(self.table_id)

        if not os.path.exists(os.path.dirname(log_file)):
            try:
                os.makedirs(os.path.dirname(log_file))
            except OSError as exc:  # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise

            open(log_file, "w")

        # Create the Handler for logging data to a file
        file_logger_handler = logging.FileHandler(log_file)
        file_logger_handler.setLevel(logging.DEBUG)
        file_logger_handler.setFormatter(logger_formatter)
        logger.addHandler(file_logger_handler)

        self.process_status_folder = "{}/process_status/".format(self.table_id)
        self.gcs_process_status_folder_blob = self.gcs_bucket.blob(self.process_status_folder)

        self.extract_status_file = "{}/process_status/extract_status_file.json".format(self.table_id)
        self.gcs_extract_status_file_blob = self.gcs_bucket.blob(self.extract_status_file)
        self.local_extract_status_file_path = Path(self.extract_status_file)


        self.log_file = log_file
        self.gcs_log_file_blob = self.gcs_bucket.blob(self.log_file)
        self.logger_name = 'billing-export'


        print("log file is : {}".format(self.log_file))

    # Check if config file has all needed the keys
    def check_config(self):
        """
        Check if the config file has all required keys
        :return: True if valid, False if not valid
        """
        try:
            # Try to get all the required keys
            if (self.project is not None
                    and self.dataset_id is not None
                    and self.table_id is not None
                    and self.bucket_name is not None):
                logger.info("{} - All required config keys are set".format(self.table_id))
                return True
            else:
                logger.error("{} - PLEASE MAKE SURE TO SET CONFIG VALUES: source_project_id, source_dataset_id, source_table_id, destination_bucket ".format(self.table_id))
                raise Exception("{} - PLEASE MAKE SURE TO SET CONFIG VALUES: source_project_id, source_dataset_id, source_table_id, destination_bucket ".format(self.table_id))
        except KeyError:
            raise


def write_to_local_status_file(filename,data):

    if not os.path.exists(os.path.dirname(filename)):
        try:
            os.makedirs(os.path.dirname(filename))
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    with open(filename, "w") as f:
        f.write(json.dumps(data, indent=4, sort_keys=False))
        f.close()


def extract_status_file_exists(config_data):
    logger.info("{} - config_data.bucket_name: {}  ,config_data.gcs_extract_status_file_blob.exists() :{}".format(config_data.table_id, config_data.bucket_name, config_data.gcs_extract_status_file_blob.exists()))

    if config_data.gcs_extract_status_file_blob.exists() or config_data.local_extract_status_file_path.exists():
        logger.debug("{} - gcs extract file {} or local status file: {} exists".format(config_data.table_id, config_data.gcs_extract_status_file_blob, config_data.local_extract_status_file_path))
        return True
    else:
        logger.debug("{} - extract status file DO NOT exist.".format(config_data.table_id))
        return False


def get_latest_extract_date_from_statusfile(config_data):
    global extract_status_json_data

    if config_data.gcs_extract_status_file_blob.exists():

        gcs_json_data_string = config_data.gcs_extract_status_file_blob.download_as_string()
        gcs_extract_status_json_data = json.loads(gcs_json_data_string)
        gcs_extract_records = gcs_extract_status_json_data['extract_status']
        sorted_extract_list = sorted(gcs_extract_records, key=lambda i: datetime.strptime(i['export_date_partition'], '%Y%m%d'), reverse=True)
        gcs_latest_record = sorted_extract_list[0]
        gcs_latest_export_start_datestring = gcs_latest_record["export_date_partition"]
        gcs_latest_export_start_date = datetime.strptime(gcs_latest_export_start_datestring, "%Y%m%d")

    else:

        gcs_extract_status_json_data = set_extract_status_json_data()
        gcs_latest_export_start_datestring = "19990101"
        gcs_latest_export_start_date = datetime.strptime(gcs_latest_export_start_datestring, "%Y%m%d")
        logger.debug("{} - gcs extract status file do not exist .. creating file in gcs".format(config_data.table_id))

    if config_data.local_extract_status_file_path.exists():

        try:
            with open(config_data.local_extract_status_file_path, 'r') as fin:
                local_json_data_string = fin.read()
                fin.close()

            local_extract_status_json_data = json.loads(local_json_data_string)
            local_extract_records = local_extract_status_json_data['extract_status']
            local_sorted_extract_list = sorted(local_extract_records, key=lambda i: datetime.strptime(i['export_date_partition'], '%Y%m%d'), reverse=True)
            local_latest_record = local_sorted_extract_list[0]
            local_latest_export_start_datestring = local_latest_record["export_date_partition"]
            local_latest_export_start_date = datetime.strptime(local_latest_export_start_datestring, "%Y%m%d")

        except JSONDecodeError:
            create_local_extract_status_file(config_data.extract_status_file)
            local_extract_status_json_data = set_extract_status_json_data()

            local_latest_export_start_datestring = "19990101"
            local_latest_export_start_date = datetime.strptime(local_latest_export_start_datestring, "%Y%m%d")

        except IndexError:
            local_extract_status_json_data = set_extract_status_json_data()
            local_latest_export_start_datestring = "19990101"
            local_latest_export_start_date = datetime.strptime(local_latest_export_start_datestring, "%Y%m%d")

    else:

        logger.debug("{} - local extract status file do not exist .. creating file in local".format(config_data.table_id))
        local_extract_status_json_data = set_extract_status_json_data()
        create_local_extract_status_file(config_data.extract_status_file)
        local_latest_export_start_datestring = "19990101"
        local_latest_export_start_date = datetime.strptime(local_latest_export_start_datestring, "%Y%m%d")

    if gcs_latest_export_start_date is not None or local_latest_export_start_date is not None:
        logger.info("{} - gcs extract file::latest updated export date:{} ,  local extract file::latest updated export date:{} ".format(config_data.table_id, gcs_latest_export_start_date, local_latest_export_start_date))

        if gcs_latest_export_start_date > local_latest_export_start_date:

            latest_extract_date = gcs_latest_export_start_datestring
            extract_status_json_data = gcs_extract_status_json_data

            logger.debug("{} - gcs extract file has latest updated export date:{} ".format(config_data.table_id, latest_extract_date))

        else:

            latest_extract_date = local_latest_export_start_datestring
            extract_status_json_data = local_extract_status_json_data
            logger.debug("{} - local extract file has latest updated export date:{} ".format(config_data.table_id, latest_extract_date))

    else:

        latest_extract_date = "19990101"
        logger.debug("{} - gcs and local extract files do not exist or files are empty. Latest extract date is set to :{} ".format(config_data.table_id, latest_extract_date))

    return latest_extract_date


def get_latest_extract_record(json_data):
    extract_records = json_data['extract_status']
    sorted_extract_list = sorted(extract_records, key=lambda i: datetime.strptime(i['export_date_partition'], '%Y%m%d'), reverse=True)
    latest_record = sorted_extract_list[0]
    return latest_record


def start_extract_process():
    global extract_status_json_data

    # Check if bucket "billing_extract_project" exists
    if lookup_extract_bucket(config_data) is None:
        logger.error("\n{} - {} BUCKET DO NOT EXIST. CREATE {} BUCKET AND TRY AGAIN ...".format(config_data.table_id, config_data.bucket_name, config_data.bucket_name))
        # If bucket "billing_extract_project" DOES NOT EXIST, raise error and exit
        raise Exception(
            "\n{} - {} BUCKET DOES NOT EXIST. CREATE {} BUCKET AND TRY AGAIN ...".format(config_data.table_id, config_data.bucket_name, config_data.bucket_name))

    else:
        # if Bucket exists, check if process_extract_status.json file exists in the bucket
        # if file exists,
        #       1.verify for failed runs and rerun the extract for those partitions.
        #       2.If start date is provided as argument(this is adhoc run), run for that date partition.
        #           else Run for delta extract
        logger.debug("\n{} - {} BUCKET EXISTS. Proceeding to check if {} status file exists ...".format(config_data.table_id, config_data.bucket_name, config_data.extract_status_file))
        export_end_date = datetime.strftime(datetime.now() + timedelta(1), '%Y%m%d')

        if extract_status_file_exists(config_data):
            logger.debug("{} - Extract status file exists .. ".format(config_data.table_id))

            if opts.historical_run:
                logger.debug("{} - Historical_run argument is true .. proceeding with historical run ...".format(config_data.table_id))

                export_start_date = "19990101"

                logger.debug("{} - Extract run for start_date : {} and end_date is {}".format(config_data.table_id, export_start_date, export_end_date))

            elif (opts.export_start_date is None) and (opts.export_end_date is None) and ((opts.historical_run is None) or (opts.historical_run is False)):

                logger.info("{} - Only config_file is provided .. proceeding with delta or historical based on partitions inside status file ...".format(config_data.table_id))

                export_start_date = get_latest_extract_date_from_statusfile(config_data)

                logger.debug("{} - Extract run for start_date : {} and end_date is {}".format(config_data.table_id, export_start_date, export_end_date))

            # else part is the adhoc run where export_start_date provided as argument
            else:
                export_start_date = opts.export_start_date
                export_end_date = opts.export_end_date

        else:
            # check if the process_status_folder exists, if does not exist-
            #   1.create folder blob
            #   2.create empty status file
            #   3.run historical extract
            logger.warning("{} - status file DOES NOT EXIST .. proceeding with status folder check ...".format(config_data.table_id))

            if config_data.gcs_process_status_folder_blob.exists() is False:
                logger.warning("{} - Extract folder DOES NOT EXIST .. proceeding with creating the folder".format(config_data.table_id))
                create_folder_in_bucket(config_data.gcs_process_status_folder_blob)

            create_local_extract_status_file(config_data.extract_status_file)

            export_start_date = "19990101"
            extract_status_json_data = set_extract_status_json_data()

            logger.debug("{} - Historical run from date partition: {} to {} ".format(config_data.table_id, export_start_date, export_end_date))

    logger.info("{} - ... starting extract process ....\n".format(config_data.table_id))
    extract_billing(export_start_date, export_end_date)
    logger.info("{} - ... Completed extract process successfully....\n".format(config_data.table_id))

    logger.info("{} - ... Re-run Failed Partions check started... ....\n".format(config_data.table_id))
    rerun_failed_partions_export(opts)
    logger.info("{} - ... Completed Re-run Failed Partions successfully....\n".format(config_data.table_id))


def create_folder_in_bucket(folder):

    folder.upload_from_string('')


def set_extract_status_json_data():
    data = {}
    data['extract_status'] = []
    return data


def create_local_extract_status_file(filename):
    if not os.path.exists(os.path.dirname(filename)):
        try:
            os.makedirs(os.path.dirname(filename))
        except OSError as exc:
            # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    with open(filename, "w") as f:
        data = set_extract_status_json_data()
        f.write(json.dumps(data))


def lookup_extract_bucket(config_data):
    return config_data.storage_client.lookup_bucket(config_data.bucket_name) is not None


def verify_lines_in_export_json(config_data, export_start_date):
    prefix = "{}/{}/".format(config_data.table_id,export_start_date)
    blobs = config_data.gcs_bucket.list_blobs(prefix=prefix)

    total_bytes_written = 0
    for blob in blobs:
        if blob.name.startswith("{}/{}/billing-export-".format(config_data.table_id, export_start_date)):
            temp_file = "{}/process_status/temp.json".format(config_data.table_id)
            blob.download_to_filename(temp_file)

            rows = sum(1 for line in open(temp_file))
            os.remove(temp_file)
            logger.debug("{} - No of rows in the destination export JSON:{}/{}/{} for date partition:{} is : {} rows".format(config_data.table_id, config_data.table_id,export_start_date,blob.name.replace(prefix, ""),export_start_date,rows))
            bytes = get_extract_json_size(blob)
            total_bytes_written = bytes + total_bytes_written

    return total_bytes_written


def get_extract_json_size(blob):

    return blob.size


def extract_partition(config_data, table_ref, destination_uri, extract_config, export_start_date):

    total_bytes_written = 0
    extract_job = config_data.big_query_client.extract_table(
            table_ref,
            destination_uri,
            job_config=extract_config,
            # Location must match that of the source table.
            location="US"
        )  # API request
    try:
        extract_job.result()  # Waits for job to complete.
        logger.debug(
                "{} - Exported {}:{}.{} to {}".format(config_data.table_id, config_data.project, config_data.dataset_id, config_data.table_id+export_start_date, destination_uri)
            )
        destination_table = config_data.big_query_client.get_table(table_ref)

        logger.debug("{} - No of rows extracted from date partition:{} of source table:{} : {} rows".format(config_data.table_id, export_start_date,config_data.table_id,destination_table.num_rows))

        total_bytes_written = verify_lines_in_export_json(config_data, export_start_date)

        success = True
    except:
        success = False

    return success, total_bytes_written


def get_partitions(config_data, export_start_date, export_end_date):
    # date partition query

    partition_query = str("""select distinct format_date('%Y%m%d',_PARTITIONDATE)as parts   
                from `{}.{}.{}`
                where format_date('%Y%m%d',_PARTITIONDATE) >= "{}"
                and format_date('%Y%m%d',_PARTITIONDATE) < "{}" 
                order by parts asc;""").format(config_data.project, config_data.dataset_id, config_data.table_id,
                                               export_start_date, export_end_date)
    logger.debug("\n{} - Partition Query: \n{}\n".format(config_data.table_id, partition_query))

    try:
        partitions = config_data.big_query_client.query(partition_query, location="US")
    except:
        logger.error("{} - Partition Query didnot execute. Please check and try again.".format(config_data.table_id))
        raise Exception(
            "Partition Query didnot execute. Please check and try again.")

    return partitions


def get_dataset_ref(config_data):
    return config_data.big_query_client.dataset(config_data.dataset_id, project=config_data.project)


def extract_billing(export_start_date, export_end_date):
    global extract_status_json_data

    partitions = get_partitions(config_data, export_start_date, export_end_date)

    dataset_ref = get_dataset_ref(config_data)

    for part in partitions.result():

        table_ref = dataset_ref.table(config_data.table_id + "$" + part[0])
        destination_uri = "gs://{}/{}/{}/{}".format(config_data.bucket_name, config_data.table_id, part[0], "billing-export-*.json")

        extract_config = bigquery.job.ExtractJobConfig()
        extract_config.destination_format = bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON
        export_start_date = part[0]

        status = "started"
        total_bytes_written = 0

        update_extract_status_json(status, export_start_date, total_bytes_written)
        write_to_local_status_file(config_data.extract_status_file,extract_status_json_data)

        success, total_bytes_written = extract_partition(config_data, table_ref, destination_uri, extract_config, export_start_date)

        if success:
            status = "success"

            update_extract_status_json(status, export_start_date, total_bytes_written)
            write_to_local_status_file(config_data.extract_status_file,extract_status_json_data)
            logger.debug("{} - Export partition completed successfully for : {} \n".format(config_data.table_id, export_start_date))



def update_extract_status_json(status, export_start_date, bytes):
    # if status is "success", append "extract_status", "run_timestamp", "export_date_partition" to the file. There is no key for "status" at this point.
    # else if status is "success", insert the record with id as status="success"
    global extract_status_json_data
    run_timestamp = datetime.utcnow().strftime("%Y%m%d %H:%M:%S.%f")[:-3]

    if status is "started":

        if not any(tag['export_date_partition'] == export_start_date for tag in extract_status_json_data['extract_status']):

            extract_status_json_data['extract_status'].insert(0, {
                            "run_timestamp": run_timestamp,
                            "export_date_partition": export_start_date
                    })

    elif status is "success":

        index = next((index for (index, d) in enumerate(extract_status_json_data['extract_status']) if d['export_date_partition'] == export_start_date), None)
        extract_status_json_data['extract_status'][index]['bytes'] = bytes
        extract_status_json_data['extract_status'][index]['status'] = "success"



def write_to_gcs_status_file(config_data, extract_status_json_data):
    config_data.gcs_extract_status_file_blob.upload_from_string(json.dumps(extract_status_json_data, indent=4, sort_keys=False))


def upload_file_to_gcs(destination_blob, filename):
    destination_blob.upload_from_filename(filename)


def get_bytes_from_status_file(extract_status_json_data, export_date):
    for data in extract_status_json_data['extract_status']:
        if data['export_date_partition'] == export_date:
            return data['bytes']


def gcs_extract_json_blob_exists(config_data, export_date):

    bytes_from_status_file = get_bytes_from_status_file(extract_status_json_data, export_date)
    prefix = "{}/{}".format(config_data.table_id,export_date )
    blobs = config_data.gcs_bucket.list_blobs(prefix=prefix)

    total_bytes = 0
    for blob in blobs:
        if blob.name.startswith("{}/{}/billing-export-".format(config_data.table_id, export_date)):

            bytes = get_extract_json_size(blob)
            total_bytes = bytes + total_bytes

    logger.debug('{} - Bytes of export partition: {} from STATUS FILE: {} bytes'.format(config_data.table_id, export_date, bytes_from_status_file))

    logger.debug('{} - Bytes of export partition : {} from GCS LOCATION: {} bytes'.format(config_data.table_id, export_date, total_bytes))

    return bytes_from_status_file == total_bytes


def get_all_partitions():
    partition_query = str("""select distinct format_date('%Y%m%d',_PARTITIONDATE)as parts   
                from `{}.{}.{}`
                order by parts asc;""").format(config_data.project, config_data.dataset_id, config_data.table_id)
    logger.debug("\n{} - Rerun failed partitions: Partition Query: get all partitions: \n{}\n".format(config_data.table_id, partition_query))

    try:
        partitions = config_data.big_query_client.query(partition_query, location="US")
    except:
        logger.error("{} - Partition Query didnot execute. Please check and try again.")
        raise Exception(
            "Partition Query didnot execute. Please check and try again.")

    return partitions


def gcs_json_export_file_exists(gcs_json_export_file, gcs_json_export_folder):

    gcs_json_export_file_blob = config_data.gcs_bucket.blob(gcs_json_export_file)

    if gcs_json_export_file_blob.exists():

        return True

    else:
        logger.debug("{} - Rerun Failed Partitions :: EXPORT FILE:: {} DOES NOT EXIST .. proceeding with creating the folder".format(config_data.table_id, gcs_json_export_file))
        config_data.gcs_extract_status_file_blob.exists()
        gcs_json_export_folder_blob = config_data.gcs_bucket.blob(gcs_json_export_folder)

        if gcs_json_export_folder_blob.exists() is False:
            logger.debug("{} - Rerun Failed Partitions :: EXPORT FOLDER:: {} DOES NOT EXIST .. proceeding with creating the folder".format(config_data.table_id, gcs_json_export_folder))
            create_folder_in_bucket(gcs_json_export_folder_blob)

        return False


def rerun_failed_partions_export(opts):
    global extract_status_json_data

    if (opts.export_start_date is None) and (opts.export_end_date is None) and ((opts.historical_run is None) or (opts.historical_run is False)):
        if extract_status_json_data is not None:
            # Read all the records which do not have status key. Run the extract process and update the status file
            for all_records in extract_status_json_data['extract_status']:
                if 'status' not in all_records:

                    failed_run = all_records
                    export_start_date = failed_run["export_date_partition"]

                    date = datetime.strptime(export_start_date, "%Y%m%d")
                    end_date = date + timedelta(days=1)
                    export_end_date = datetime.strftime(end_date, "%Y%m%d")

                    logger.debug("{} - Failed Partition export: export_start_date: {}... export_end_date: {} ...".format(config_data.table_id, export_start_date, export_end_date))

                    extract_billing(export_start_date, export_end_date)

            partitions = get_all_partitions()

            dataset_ref = get_dataset_ref(config_data)

            for part in partitions.result():

                export_date = part[0]
                destination_uri = "gs://{}/{}/{}/{}".format(config_data.bucket_name, config_data.table_id, export_date, "billing-export-*.json")

                if gcs_extract_json_blob_exists(config_data, export_date) is False:

                    extract_config = bigquery.job.ExtractJobConfig()
                    extract_config.destination_format = bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON

                    status = "started"
                    total_bytes_written = 0

                    update_extract_status_json(status, export_date, total_bytes_written)
                    write_to_local_status_file(config_data.extract_status_file,extract_status_json_data)

                    table_ref = dataset_ref.table(config_data.table_id + "$" + export_date)
                    success, total_bytes_written = extract_partition(config_data, table_ref, destination_uri, extract_config, export_date)


                    if success:

                        status = "success"

                        update_extract_status_json(status, export_date, total_bytes_written)
                        write_to_local_status_file(config_data.extract_status_file,extract_status_json_data)
                        logger.debug("{} - Export partition completed successfully for : {}\n".format(config_data.table_id, export_date))




def parse_args():
    """Parse argv into usable input."""
    # Setup argument parser

    # Instantiate the parser
    parser = argparse.ArgumentParser(description='Optional app description')

    # optional positional argument
    parser.add_argument('--config_file', type=str, nargs='?',
                        help='An optional config json file to define source_project_id, source_dataset_id, source_table_id, destination_bucket')

    # Optional positional argument
    parser.add_argument('--export_start_date', type=str, nargs='?',
                        help='An optional string for export start date with format yyyyMMdd')

    # Optional positional argument
    parser.add_argument('--export_end_date', type=str, nargs='?',
                        help='An optional string for export end date with format yyyyMMdd')

    # Optional argument
    parser.add_argument('--historical_run', type=bool, nargs='?',
                        help='An optional boolean value for historic run')

    opts = parser.parse_args()

    return opts


def signal_handler(signum, frame):

    logger.critical('{} - got {} termination signal, saving local status file, gcs status file, log file ...'.format(config_data.table_id, signum))

    write_to_gcs_status_file(config_data,extract_status_json_data)
    write_to_local_status_file(config_data.extract_status_file,extract_status_json_data)
    upload_file_to_gcs(config_data.gcs_log_file_blob, config_data.log_file)
    temp_file = "{}/process_status/temp.json".format(config_data.table_id)
    if os.path.exists(temp_file): os.remove(temp_file)

    logger.critical('{} - Gracefully exiting ............'.format(config_data.table_id))

    os._exit(1)


def read_exporter_config(opts):

    config_file = "config.json"

    if opts.config_file is None:

        command = "gcloud compute instances describe $(hostname) --project=$(gcloud config get-value project) --zone=$(gcloud config get-value compute/zone) --flatten='"'metadata[exporter-config]'"'"
        process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
        (process_output,  error) = process.communicate()
        file = open(config_file, "w")
        file.write(process_output.decode('utf-8'))
        file.close()

        with open(config_file, 'r') as fin:
            data = fin.read().splitlines(True)
        with open(config_file, 'w') as fout:
            fout.writelines(data[1:])

        exporter_config = config_file

    else:
        exporter_config = opts.config_file

    if exporter_config is not None:
        with open(exporter_config) as json_data:
            exporter_config_data = json.load(json_data)
            json_data.close()
    else:
        logger.error("{} - Config data is not set")

    # Delete the config file created when run with no args after copying the instance metadata config to exporter_config_data
    if opts.config_file is None:
        os.remove(config_file)

    return exporter_config_data


if __name__ == '__main__':
    # Execute main loop

    opts = parse_args()

    exporter_config = read_exporter_config(opts)
    config_data = Config(exporter_config)

    if config_data.check_config():

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        start_extract_process()

        if extract_status_json_data is not None:

            write_to_gcs_status_file(config_data,extract_status_json_data)
            logger.debug("{} - Extract status file is saved on gcs".format(config_data.table_id))

        upload_file_to_gcs(config_data.gcs_log_file_blob, config_data.log_file)
        logger.debug("{} - Extract log status file is saved on gcs".format(config_data.table_id))

    else:

        raise Exception("Make sure to set all the config values - source_project_id, source_dataset_id, source_table_id, destination_bucket")
