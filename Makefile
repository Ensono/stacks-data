setup_dev_environment:
	echo "PYTHONPATH=." > .env
	poetry install
	poetry run pre-commit install

test:
	poetry run python -m pytest tests

test_coverage:
	poetry run coverage run -m pytest tests
	poetry run coverage xml -i
	poetry run coverage report -m --fail-under 80

pre_commit:
	poetry run pre-commit run --all-files
