import boto3.session
import setuptools,boto3
import boto3
import re
#from packaging import version

version = '10.0.8'
def get_latest_whl(bucket_name, prefix=''):
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    whl_files = [item['Key'] for item in response.get('Contents', []) if item['Key'].endswith('.whl')]
    print(f'List of whl file(s) present :: {whl_files}')

    if not whl_files or not len(whl_files):
        return "0"

    # Extract versions and find the latest one
    def extract_version(file_name):
        # Extract the version from the format bytecodeairflow-5.0.9-py3-none-any.whl
        match = re.search(r'bytecodeairflow-([0-9]+\.[0-9]+(?:\.[0-9]+)*)-', file_name)
        return version.parse(match.group(1)) if match else version.parse("0.0.0")

    latest_file = max(whl_files, key=extract_version)

    latest_whl = latest_file.split('/')[-1]

    latest_version = latest_whl.split("-")[1].replace(".","")
    
    return latest_version


def get_new_version(cur_version):
    new_ver = list(f'{(int(cur_version) + 1):03d}')
    return ".".join(new_ver)


def build_wheel():
    '''
        Upload lastest version whl file created on s3 bucket
    '''

    s3_bucket = 'airflow-config-bucket'
    s3_key = "wheel_packages/"

    # latest_version = get_latest_whl(bucket_name=s3_bucket, prefix=s3_key)
    # nextversion = get_new_version(latest_version)
    # print(f'Next version is :: {nextversion}')
    setuptools.setup(
        name="bytecodeairflow",
        version=version,
        author="sumit kumar",
        author_email="sumit.kumar1@cloudkeeper.com",
        description="Airflow dynamic pipeline framework",
        packages=setuptools.find_packages(),
        install_requires=[
            'pymongo>=4.6.1',
        ],
        package_data={
            'bytecodeairflow': [
                'configurations/*.json'
            ],
        },
        python_requires='>=3.10'
)

build_wheel()
