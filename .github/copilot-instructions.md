# Copilot Instructions for Stacks Data

## Project Overview

**stacks-data** is a Python package for the Ensono Stacks Data Platform, providing utilities and CLI tools to support data engineering workloads. The project focuses on Azure-based data platforms, PySpark operations, and data quality management.

## Core Technologies

- **Python 3.9+** (minimum version)
- **PySpark** (3.4.3+, <4.0.0) with Delta Lake integration
- **Azure SDK** (Data Factory, Storage, Identity)
- **Great Expectations** (data quality framework)
- **Poetry** for dependency management
- **Click** for CLI implementation

## Project Structure

```
stacks/data/
├── cli/                    # Datastacks CLI implementation
├── generate/               # Data workload generation (templates)
├── platforms/
│   ├── azure/             # Azure-specific utilities (ADF, ADLS, Blob)
│   ├── fabric/            # Microsoft Fabric utilities
│   └── common/            # Platform-agnostic data lake operations
├── pyspark/               # PySpark utilities and data quality
│   └── data_quality/      # Great Expectations integration
├── behave/                # BDD test fixtures and shared steps
├── logger.py              # Logging configuration
└── utils.py               # Common utilities
```

## Development Guidelines

### Code Style

1. **Formatting**: Use Black with 120 character line length

   ```python
   # Run formatting
   poetry run black .
   ```

2. **Linting**: Use flake8 with 120 character max line length

   ```python
   # Run linting
   poetry run flake8
   ```

3. **Docstrings**: Follow Google style convention

   - Required for: functions, classes, modules
   - Skip for: test files, `__init__.py` without logic
   - Example:

   ```python
   def my_function(param1: str, param2: int) -> bool:
       """Short description of function.

       Longer description if needed, explaining the purpose
       and any important details.

       Args:
           param1: Description of param1.
           param2: Description of param2.

       Returns:
           Description of return value.

       Raises:
           ValueError: When something goes wrong.
       """
   ```

4. **Type Hints**: Always use type hints for function signatures

   ```python
   from typing import Optional, Any, Dict

   def process_data(data: DataFrame, config: Dict[str, Any]) -> Optional[DataFrame]:
       ...
   ```

5. **Naming Conventions**:
   - Functions/methods: `snake_case`
   - Classes: `PascalCase`
   - Constants: `UPPER_SNAKE_CASE`
   - Private members: prefix with `_`

### Testing Requirements

1. **Test Framework**: pytest with pytest-mock
2. **Test Location**: `tests/unit/` matching source structure
3. **Fixtures**: Use conftest.py for shared fixtures
4. **Spark Testing**:
   - Use session-scoped `spark` fixture from `tests/unit/conftest.py`
   - Use `chispa` for DataFrame comparisons
5. **Coverage**: Aim for high coverage, use `coverage` tool
6. **File Naming**: Prefix test files with `test_`

Example test structure:

```python
from pytest import fixture
import pytest

def test_my_function(spark, mocker):
    """Test description following function name."""
    # Arrange
    mock_obj = mocker.patch('module.function')

    # Act
    result = my_function(spark, param)

    # Assert
    assert result == expected
    mock_obj.assert_called_once()
```

### Module-Specific Guidelines

#### PySpark Utilities (`stacks/data/pyspark/`)

- Always use `get_spark_session()` for SparkSession creation
- Configure Delta Lake integration via provided constants
- Use `camel_to_snake()` for column name normalization
- Handle optional environment variable substitution with `substitute_env_vars()`

#### Azure Platform (`stacks/data/platforms/azure/`)

- Use Azure Identity for authentication (DefaultAzureCredential)
- Handle Azure SDK exceptions appropriately
- Implement retry logic for transient failures
- Use constants from `constants.py` for configuration

#### CLI (`stacks/data/cli/`)

- Use Click decorators for commands and options
- Implement help options (`-h`, `--help`)
- Use structured logging via `setup_logger()`
- Validate configurations before processing

#### Data Quality (`stacks/data/pyspark/data_quality/`)

- Great Expectations integration
- JSON-based configuration
- Publish results to Delta tables
- Support multiple datasource types

### Commit Standards

Use **Conventional Commits** format:

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

Types:

- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Formatting, missing semi-colons, etc.
- `refactor`: Code restructuring
- `test`: Adding tests
- `chore`: Maintenance tasks (deps, build, etc.)

Examples:

```
feat(pyspark): add support for reading Parquet files with schema evolution
fix(azure): handle authentication timeout in ADLS operations
chore(deps): update Azure SDK to latest version
test(cli): add unit tests for generate command
```

### Pre-commit Hooks

The project uses pre-commit hooks that run automatically:

- YAML validation
- End-of-file fixer
- Trailing whitespace trimmer
- yamllint
- flake8
- black
- pydocstyle

Ensure Python 3.10 is available for pre-commit environments.

### Optional Dependencies

The package has modular installation:

- **Core**: PySpark + Azure operations (always installed)
- **`[behave]`**: BDD testing utilities
- **`[cli]`**: Datastacks CLI tool
- **`[data-quality]`**: Great Expectations integration

When adding features:

- Keep core dependencies minimal
- Add specialized dependencies as optional
- Update `pyproject.toml` extras accordingly
- Document installation requirements

### Common Patterns

#### Spark Session Creation

```python
from stacks.data.pyspark.pyspark_utils import get_spark_session

spark = get_spark_session("my_app_name", spark_config={"key": "value"})
```

#### Reading Data Sources

```python
from stacks.data.pyspark.pyspark_utils import read_datasource

df = read_datasource(
    spark=spark,
    file_format="delta",
    data_path="path/to/data",
    options={"mergeSchema": "true"}
)
```

#### Azure Authentication

```python
from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient

credential = DefaultAzureCredential()
adf_client = DataFactoryManagementClient(credential, subscription_id)
```

#### Logging

```python
import logging

logger = logging.getLogger(__name__)
logger.info("Processing data...")
logger.error("Error occurred", exc_info=True)
```

### File Organization

When creating new modules:

1. Add `__init__.py` to make it a package
2. Keep related functionality together
3. Separate platform-specific from common code
4. Add corresponding test file in `tests/unit/`
5. Update documentation if adding public APIs

### Configuration Management

- Use **Pydantic** models for configuration validation
- Support YAML configuration files
- Allow environment variable substitution
- Provide sensible defaults
- Validate early, fail fast

Example:

```python
from pydantic import BaseModel, validator

class MyConfigModel(BaseModel):
    name: str
    count: int = 10

    @validator('count')
    def count_must_be_positive(cls, v):
        if v <= 0:
            raise ValueError('count must be positive')
        return v
```

### Error Handling

- Use specific exception types
- Provide helpful error messages
- Log errors with context
- Clean up resources in finally blocks
- Don't swallow exceptions silently

```python
try:
    result = risky_operation()
except SpecificError as e:
    logger.error(f"Operation failed: {e}", exc_info=True)
    raise
finally:
    cleanup_resources()
```

### Performance Considerations

- Leverage Spark's lazy evaluation
- Minimize shuffles in Spark operations
- Cache DataFrames when reused multiple times
- Use appropriate file formats (prefer Delta/Parquet)
- Partition large datasets appropriately

### Security Best Practices

- Never commit secrets or credentials
- Use Azure Identity for authentication
- Rely on environment variables for sensitive config
- Validate all user inputs
- Follow least-privilege principle for Azure permissions

### Security and Compliance

**IMPORTANT**: All code suggestions and operations must comply with security and compliance requirements.

See [Security Instructions](./copilot-security-instructions.md) for detailed guidelines on:

- GPG commit signing requirements (mandatory)
- Branch protection and pull request workflows
- Production configuration change control
- Authentication and authorization controls
- Security standards compliance (ISO 27001, NIST, PCI DSS, GDPR, etc.)
- Audit and logging requirements
- Incident response procedures

**Never bypass security controls or suggest workarounds that violate security policies.**

## Related Resources

- [Stacks Documentation](https://stacks.ensono.com/docs/workloads/azure/data/intro_data_azure)
- [Datastacks CLI Docs](https://stacks.ensono.com/docs/workloads/azure/data/data_engineering/datastacks)
- [GitHub Repository](https://github.com/Ensono/stacks-data)
- [Azure Data Platform](https://github.com/Ensono/stacks-azure-data)

## Getting Started for Contributors

1. Clone the repository
2. Install Poetry: `pip install poetry`
3. Install dependencies: `poetry install --all-extras`
4. Install pre-commit hooks: `poetry run pre-commit install`
5. Run tests: `poetry run pytest`
6. Check formatting: `poetry run black . --check`
7. Run linting: `poetry run flake8`

## Version Management

- Versioning managed by **commitizen**
- Follows **Conventional Commits** for automatic versioning
- Don't manually update version in `pyproject.toml`
- Use `cz bump` to increment version based on commits

## CI/CD Considerations

When modifying CI/CD or build processes:

- Check `.github/workflows/` (if exists)
- Update `build/azDevOps/` pipeline definitions
- Test taskctl commands defined in `taskctl.yaml`
- Ensure compatibility with Azure DevOps pipelines
- Update `Makefile` targets if needed
