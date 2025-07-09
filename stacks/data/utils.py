"""Utilities module.

This module provides a collection of generic helper functions and utilities to support Stacks Data.
"""
import json
import logging
import os
import re
import requests

from collections import Counter
from pathlib import Path
from requests.exceptions import RequestException

logger = logging.getLogger(__name__)


def find_placeholders(input_str: str) -> list[str]:
    """Finds all placeholders in the form {PLACEHOLDER} in the input string.

    Args:
        input_str: The input string possibly containing placeholders.

    Returns:
        A list of all found placeholders.
    """
    return re.findall(r"\{(.+?)\}", input_str)


def substitute_env_vars(input_str: str) -> str:
    """Replaces placeholders in the form {PLACEHOLDER} with corresponding environment variable values.

    Args:
        input_str: The input string possibly containing placeholders.

    Returns:
        The input string with placeholders replaced with environment variable values.
    """
    placeholders = find_placeholders(input_str)

    for placeholder in placeholders:
        env_var_value = os.getenv(placeholder)
        if env_var_value:
            input_str = input_str.replace("{" + placeholder + "}", env_var_value)

    return input_str


def filter_files_by_extension(paths: list[str], extension: str) -> list[str]:
    """Returns paths with the given extension.

    Args:
        paths: List of file paths.
        extension: File extension to filter by. Dot is not necessary, e.g. you can pass "csv".

    Returns:
        A list of file paths that end with the given extension.

    """
    if not extension.startswith("."):
        extension = f".{extension}"

    return [path for path in paths if path.endswith(extension)]


def camel_to_snake(camel_str: str) -> str:
    """Converts a camelCase string to snake_case.

    Args:
        camel_str: A string in camelCase format.

    Returns:
        The string converted to snake_case format.
    """
    snake_str = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", camel_str)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", snake_str).lower()


def config_uniqueness_check(config_list: list[dict], unique_key: str):
    """Checks dictionaries within the list and returns true if the unique_key provided is actually unique.

    Args:
        config_list: List of dictionaries containing config
        unique_key: Key to check for uniqueness
    """
    key_counter = Counter(i[unique_key] for i in config_list)
    duplicates = [key for key, count in key_counter.items() if count > 1]
    if duplicates:
        logger.error(f"{unique_key} values are not unique: {', '.join(map(str, duplicates))}")
        return False
    return True


def load_config_as_dict(path: str) -> dict:
    """Gets the contents of the required JSON resource and converts to a dictionary.

    Args:
        path: Path of the required resource (e.g 'folder1/resource1.json')

    Returns:
        Config as a dictionary
    """
    with open(path) as f:
        return json.load(f)


def load_configs_as_list(path: str) -> list[dict]:
    """Gets all JSON config files within the given path.

    Args:
        path: Path of the required resources folder within the config folder (e.g 'folder1')

    Returns:
        Configs as a list of dictionaries
    """
    config_list = []
    files = Path(path).glob("*.json")
    for file in list(files):
        with open(file) as f:
            config_list.append(json.load(f))
    return config_list


def get_latest_package_version(package_name: str) -> str:
    """Retrieve the latest version of a package from PyPI (Python Package Index).

    Makes a HTTP request to PyPI to fetch metadata about the specified package, and returns its latest version.

    Args:
        package_name (str): A PyPI package name.

    Returns:
        str: The latest version of the package found on PyPI.

    Raises:
        RequestException: If there is an error during the HTTP request to PyPI.
    """
    try:
        response = requests.get(f"https://pypi.org/pypi/{package_name}/json")
        response.raise_for_status()
        data = response.json()
        package_version = data["info"]["version"]
        return package_version
    except RequestException as e:
        logger.error(f"Error fetching package versions for {package_name}: {e}")
        raise e
