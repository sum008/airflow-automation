import subprocess
import boto3
import re
from collections import defaultdict
import os
import logging
from airflow.models import Variable

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def download_latest_whl(bucket_name, prefix):
    s3 = boto3.client('s3')

    # List all .whl files in the specified S3 bucket and prefix
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    whl_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.whl')]

    if not whl_files:
        raise FileNotFoundError("No .whl files found in the specified bucket and prefix.")

    # Extract the version number from each filename
    version_pattern = re.compile(r'([a-zA-Z_-]+)-(\d+\.\d+\.\d+)')

    whl_versions = defaultdict(list)
    for whl_file in whl_files:
        match = version_pattern.search(whl_file)
        if match:
            package_name = match.group(1)
            version = tuple(map(int, match.group(2).split('.')))
            whl_versions[package_name].append((version, whl_file))

    # Identify the .whl file with the highest version number for each package type
    latest_whl_files = {}
    for package_name, versions in whl_versions.items():
        latest_whl_file = max(versions, key=lambda x: x[0])[1]
        latest_whl_files[package_name] = latest_whl_file

    # Download the files with the highest version to the local /tmp/ directory and return their paths
    downloaded_files = {}
    for package_name, whl_file in latest_whl_files.items():
        local_path = os.path.join('/tmp', os.path.basename(whl_file))
        s3.download_file(bucket_name, whl_file, local_path)
        downloaded_files[package_name] = local_path

    return downloaded_files


def delete_whl_files(file_path):
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Deleted {file_path}")
        else:
            logger.info(f"File {file_path} not found.")
    except Exception as e:
        logger.error(f"Error deleting {file_path}: {str(e)}")


def run_subprocess(command, shell=False):
    try:
        # Pass the command as a list of arguments
        process = subprocess.Popen(command, shell=shell, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()

        # Log the output and errors
        logger.info(stdout.decode())
        logger.error(stderr.decode())

        # Check return code
        if process.returncode != 0:
            logger.error(f"Command '{command}' failed with return code {process.returncode}")
            raise subprocess.CalledProcessError(process.returncode, command)
    except subprocess.CalledProcessError as e:
        logger.error(e)
        logger.error(f"Command '{command}' failed with return code {e.returncode}")


def main():
    s3_bucket = Variable.get('airflow_bucket')
    s3_key = "wheel_packages/"

    whl_dict = download_latest_whl(bucket_name=s3_bucket, prefix=s3_key)
    for key, val in whl_dict.items():
        logger.info(f'INSTALLING :: {key}')
        logger.info(val)
        run_subprocess(['pip', 'install', val])
        logger.info(f'DONE INSTALLING :: {key}')
        delete_whl_files(file_path=val)

    shell_commands = (
        'sudo dnf install wget -y && '
        'if [ ! -f /usr/local/airflow/global-bundle.pem ]; then '
        'wget -P /usr/local/airflow https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem; '
        'fi'
    )

    run_subprocess(shell_commands, shell=True)