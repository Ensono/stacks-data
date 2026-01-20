import pytest
import yaml

from click.testing import CliRunner
from pydantic import ValidationError
from stacks.data.cli.datastacks_cli import ingest, processing

TEST_CONFIG_DIRECTORY = "tests/data/template_config/"
TEST_CONFIG_INGEST_MINIMAL = TEST_CONFIG_DIRECTORY + "test_config_ingest_minimal.yml"
TEST_CONFIG_INGEST_FULL = TEST_CONFIG_DIRECTORY + "test_config_ingest_full.yml"
TEST_CONFIG_INGEST_INVALID = TEST_CONFIG_DIRECTORY + "test_config_ingest_invalid.yml"
TEST_CONFIG_PROCESS_MINIMAL = TEST_CONFIG_DIRECTORY + "test_config_process_minimal.yml"
TEST_CONFIG_PROCESS_FULL = TEST_CONFIG_DIRECTORY + "test_config_process_full.yml"
TEST_CONFIG_PROCESS_INVALID = TEST_CONFIG_DIRECTORY + "test_config_process_invalid.yml"


@pytest.mark.parametrize(
    "function,config,dq_opt",
    [
        (ingest, TEST_CONFIG_INGEST_MINIMAL, "--no-data-quality"),
        (ingest, TEST_CONFIG_INGEST_MINIMAL, "--data-quality"),
        (processing, TEST_CONFIG_PROCESS_MINIMAL, "--no-data-quality"),
        (processing, TEST_CONFIG_PROCESS_FULL, "--data-quality"),
    ],
)
def test_cli(function, config, dq_opt):
    runner = CliRunner()
    test_config_file = "test_config.yml"
    with open(config, "r") as file:
        config_dict = yaml.safe_load(file)

    with runner.isolated_filesystem():
        with open(test_config_file, "w") as f:
            f.write(yaml.dump(config_dict))

        result = runner.invoke(function, ["--config", test_config_file, dq_opt])
        assert result.exit_code == 0


@pytest.mark.parametrize(
    "function,config,dq_opt",
    [
        (ingest, TEST_CONFIG_INGEST_INVALID, "--no-data-quality"),
        (ingest, TEST_CONFIG_INGEST_INVALID, "--data-quality"),
        (processing, TEST_CONFIG_PROCESS_INVALID, "--no-data-quality"),
        (processing, TEST_CONFIG_PROCESS_INVALID, "--data-quality"),
    ],
)
def test_cli_invalid(function, config, dq_opt):
    runner = CliRunner()
    test_config_file = "test_config.yml"
    with open(config, "r") as file:
        config_dict = yaml.safe_load(file)

    with runner.isolated_filesystem():
        with open(test_config_file, "w") as f:
            f.write(yaml.dump(config_dict))

        result = runner.invoke(function, ["--config", test_config_file, dq_opt])
        assert isinstance(result.exception, ValidationError)
