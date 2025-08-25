# AGENTS.md

This file provides guidelines for agentic coding agents operating in this repository.

## Commands

- **Run all tests**: `pytest`
- **Run a specific test file**: `pytest tests/test_api.py`
- **Run a single test**: `pytest tests/test_api.py::TestGetCryptoHistoricalData::test_successful_api_call_specific_date`
- **Linting**: `ruff check .`
- **Formatting**: `ruff format .`

## Code Style

- **Imports**: Group imports into three sections: standard library, third-party packages, and application-specific modules, separated by a blank line.
- **Formatting**: Adhere to `ruff` formatting.
- **Types**: Use type hints for function arguments and return values.
- **Naming Conventions**: Use `snake_case` for functions and variables, and `PascalCase` for classes.
- **Error Handling**: Use `try...except` blocks for operations that can fail, such as network requests or file I/O. Raise `ValueError` for invalid function arguments.
- **Docstrings**: Use docstrings for all public modules, classes, and functions.
- **Testing**: Use `pytest` for tests. Use classes to group related tests.
