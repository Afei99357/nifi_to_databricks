# Tests for NiFi to Databricks Migration Tools

This directory contains comprehensive unit and integration tests for the migration tools.

## Test Structure

- `test_migration_tools.py` - Unit tests for core migration functions
- `test_integration.py` - Integration tests using real NiFi XML files
- `conftest.py` - Shared pytest fixtures
- `README.md` - This file

## Running Tests

### Install Test Dependencies

```bash
uv add --dev pytest pytest-mock pytest-asyncio
```

### Run All Tests

```bash
# Using the test runner script
python run_tests.py

# Or directly with pytest
pytest tests/ -v
```

### Run Specific Test Categories

```bash
# Unit tests only
pytest tests/test_migration_tools.py -v

# Integration tests only
pytest tests/test_integration.py -v

# Exclude slow tests
pytest tests/ -m "not slow"

# Only integration tests
pytest tests/ -m integration
```

### Run Specific Test Classes

```bash
# Test only the build_migration_plan function
pytest tests/test_migration_tools.py::TestBuildMigrationPlan -v

# Test only orchestrate_chunked_nifi_migration
pytest tests/test_migration_tools.py::TestOrchestrateChekedNifiMigration -v
```

## Test Coverage

The tests cover:

### `build_migration_plan`
- ✅ Successful plan building with valid XML
- ✅ Error handling with invalid XML
- ✅ Empty processors handling
- ✅ Connection mapping

### `process_nifi_chunk`
- ✅ Successful chunk processing
- ✅ Batch code generation
- ✅ Individual processor fallback
- ✅ Error handling for invalid data
- ✅ Empty chunk handling

### `orchestrate_chunked_nifi_migration`
- ✅ Full end-to-end migration flow
- ✅ File creation and directory structure
- ✅ Job deployment with run_now option
- ✅ Error handling for missing files
- ✅ Workflow extraction failure handling
- ✅ Environment variable overrides

### Integration Tests
- ✅ Real NiFi XML file processing
- ✅ Performance testing for large files
- ✅ All project XML files validation

## Mocking Strategy

Tests use extensive mocking to isolate units under test:

- `@patch('tools.migration_tools.generate_databricks_code')` - Mock LLM code generation
- `@patch('tools.migration_tools.deploy_and_run_job')` - Mock Databricks API calls
- `@patch('tools.migration_tools._read_text')` - Mock file operations
- Fixture-based mocking for consistent test data

## Test Data

Sample test data includes:
- Minimal NiFi XML templates
- Chunk data structures
- Expected migration outputs
- Error scenarios

## Environment Variables

Tests automatically mock common environment variables:
- `MAX_PROCESSORS_PER_CHUNK=20`
- `ENABLE_LLM_CODE_GENERATION=true`
- `LLM_SUB_BATCH_SIZE=10`

Override using `monkeypatch.setenv()` in individual tests.

## Adding New Tests

1. **Unit Tests**: Add to `test_migration_tools.py`
   - Follow the existing class structure
   - Use descriptive test names (`test_function_scenario`)
   - Mock external dependencies

2. **Integration Tests**: Add to `test_integration.py`
   - Mark with `@pytest.mark.integration`
   - Use real data when possible
   - Mark slow tests with `@pytest.mark.slow`

3. **Fixtures**: Add reusable test data to `conftest.py`

## Continuous Integration

Tests are designed to run in CI environments:
- No external dependencies (all mocked)
- Fast execution (< 30s for full suite)
- Clear failure messages
- Proper cleanup of temp files
