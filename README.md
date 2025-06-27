# Stacks Data

**stacks-data** is a Python package built to support various functions within the Ensono Stacks Data Platform solution. The library and its associated Python-based CLI (`datastacks`) is intended to assist developers working within a deployed Stacks Data Platform, supporting common tasks such as generating new data engineering workloads and running Spark jobs.

* [Stacks Azure Data Platform - GitHub](https://github.com/Ensono/stacks-azure-data)
* [Stacks Azure Data Platform - Documentation](https://stacks.ensono.com/docs/workloads/azure/data/intro_data_azure)
* [Datastacks CLI - Documentation](https://stacks.ensono.com/docs/workloads/azure/data/data_engineering/datastacks)

## Installation

stacks-data is modular, allowing you to install only what you need, keeping the installation lightweight and efficient. By default, stacks-data installs only core functionality, focussed on Pyspark and Azure operations.

The following features require additional dependencies, which can be optionally included in your installation:

* **behave**: Utilities for executing behaviour-driven development (BDD) tests.
* **cli**: The [datastacks](https://stacks.ensono.com/docs/workloads/azure/data/data_engineering/datastacks) command line tool, to support developers generating data workloads.
* **data-quality**: Utilities for running data quality checks using the Great Expectations framework.

You can install the stacks-data package using pip - see the examples below:

```sh
# Example 1: Install only the core stacks-data package
pip install stacks-data

# Example 2: Install the stacks-data package with data quality features included
pip install stacks-data[data-quality]

# Example 3: Install the stacks-data package with all optional features included
pip install stacks-data[behave,cli,data-quality]
```
