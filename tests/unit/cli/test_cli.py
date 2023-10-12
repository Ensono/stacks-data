from click.testing import CliRunner
import yaml
from datastacks.cli.datastacks_cli import ingest, processing


TEST_CONFIG_DIRECTORY = "datastacks/tests/data/template_config/"
TEST_CONFIG_INGEST = TEST_CONFIG_DIRECTORY + "test_config_ingest.yml"
TEST_CONFIG_PROCESS = TEST_CONFIG_DIRECTORY + "test_config_process.yml"


def test_cli_ingest():
    runner = CliRunner()
    dq_options = ["--no-data-quality", "--data-quality"]
    with open(TEST_CONFIG_INGEST, "r") as file:
        config_dict = yaml.safe_load(file)

    with runner.isolated_filesystem():
        with open("test_config_ingest.yml", "w") as f:
            f.write(yaml.dump(config_dict))

        for dq_option in dq_options:
            result = runner.invoke(ingest, ["--config", "test_config_ingest.yml", dq_option])
            assert result.exit_code == 0


def test_cli_processing():
    runner = CliRunner()
    dq_options = ["--no-data-quality", "--data-quality"]
    with open(TEST_CONFIG_PROCESS, "r") as file:
        config_dict = yaml.safe_load(file)

    with runner.isolated_filesystem():
        with open("test_config_process.yml", "w") as f:
            f.write(yaml.dump(config_dict))

        for dq_option in dq_options:
            result = runner.invoke(processing, ["--config", "test_config_process.yml", dq_option])
            assert result.exit_code == 0
