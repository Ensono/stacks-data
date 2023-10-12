setup_dev_environment:
	echo "PYTHONPATH=." > .env
	poetry install
	poetry run pre-commit install

test:
	poetry run python -m pytest tests

pre_commit:
	poetry run pre-commit run --all-files
