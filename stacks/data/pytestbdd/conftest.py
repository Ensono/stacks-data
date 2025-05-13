import pytest


class ScenarioContext:
    def __init__(self):
        self.start_time = None
        self.test_run_id = None
        self.adf_run_id = None


@pytest.fixture
def context():
    return ScenarioContext()
