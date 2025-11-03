import importlib
# import pkg_resources
import importlib.resources
import importlib.util
import json
import logging
import pkgutil

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def load_config(package_name, filename, extension='json'):
    try:
        pkg_file_data = pkgutil.get_data(package=package_name, resource=filename).decode('utf-8')
        if extension == 'json':
            return json.loads(pkg_file_data)
        else:
            return pkg_file_data
    except Exception as err:
        exception_msg = f'Exception occurred while loading config :: {filename}, from package :: {package_name}, with extension :: {extension}, failure due to :: {err}'
        logger.error(exception_msg)
        raise Exception(exception_msg)


# def get_and_download_file(s3_file_path: str):
#     bucket_split = s3_file_path.split("/")
#     bucket_name, s3_key = bucket_split[0], '/'.join(bucket_split[1:])
#     aws_connection = CreateAWSConnection()
#     s3_client = aws_connection.create_aws_client(service_name="s3", region_name="aws")

#     # Create a temporary file
#     with tempfile.NamedTemporaryFile(delete=False) as temp_file:
#         temp_file_name = temp_file.name

#     # Download the file from S3 to the temporary file
#     s3_client.download_file(bucket_name, s3_key, temp_file_name)
#     return temp_file_name


def create_operator(task_config, operator_name, operator_params):
    operator_config = task_config[operator_name]
    operator_class_path = operator_config['class']
    module_name, class_name = operator_class_path.rsplit('.', 1)
    try:
        module = importlib.import_module(module_name)
        operator_class = getattr(module, class_name)
        return operator_class(**operator_params)
    except Exception as err:
        exception_msg = f'Exception occurred while creating operator :: {operator_name}, from class :: {class_name}, with params :: {operator_params}, failure due to :: {err}'
        logger.error(exception_msg)
        raise Exception(exception_msg)


# use this method in case of loading attribute from a script and the script is not in a package
def load_attribute_from_script(module_path, attribute_name):
    try:
        spec = importlib.util.spec_from_file_location("dynamic_module", module_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return getattr(module, attribute_name)
    except Exception as err:
        exception_msg = f'exception occurred while loading attribute :: {attribute_name} from script with path :: {module_path}, failure due to :: {err}'
        logger.error(exception_msg)
        raise Exception(exception_msg)


# use this method when loading attribute from package(package must be installed)
def load_attribute_from_package(package, module_path, attribute_name):
    try:
        package_path = importlib.resources.files(package)
        full_module_path = package_path.joinpath(module_path)
        spec = importlib.util.spec_from_file_location("dynamic_module", full_module_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return getattr(module, attribute_name)
    except Exception as err:
        exception_msg = f'exception occurred while loading attribute :: {attribute_name} from package :: {package}, with path :: {module_path}, failure due to :: {err}'
        logger.error(exception_msg)
        raise Exception(exception_msg)


# use this method in case loading an attribute from a class and that class is not present in a package
def load_attribute_from_class_path(module_path, class_name, attribute_name, class_params={}):
    try:
        class_instance = load_attribute_from_script(module_path, class_name)
        my_class_instance = class_instance(class_params)
        dynamic_function = getattr(my_class_instance, attribute_name)
        return dynamic_function
    except Exception as err:
        exception_msg = f'exception occurred while loading attribute :: {attribute_name} from class_path with class :: {class_name}, with path :: {module_path}, and params :: {class_params}, failure due to :: {err}'
        logger.error(exception_msg)
        raise Exception(exception_msg)


# use this method while loading an attribute from a class that is inside a package(package must be installed)
def load_attribute_from_package_class_path(package, module_path, class_name, attribute_name, class_params={}):
    try:
        class_instance = load_attribute_from_package(package, module_path, class_name)
        my_class_instance = class_instance(class_params)
        dynamic_function = getattr(my_class_instance, attribute_name)
        return dynamic_function
    except Exception as err:
        exception_msg = f'exception occurred while loading attribute :: {attribute_name} from package_class_path :: {package}, with class :: {class_name}, with path :: {module_path}, and params :: {class_params}, failure due to :: {err}'
        logger.error(exception_msg)
        raise Exception(exception_msg)
