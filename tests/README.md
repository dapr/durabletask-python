# Testing Guide

This directory contains comprehensive tests for the durabletask-python SDK, including both unit tests and end-to-end (E2E) tests.

## Quick Start

### Install Dapr CLI (if not already installed)

```bash
curl -fsSL https://raw.githubusercontent.com/dapr/cli/master/install/install.sh | /bin/bash
```

# Initialize Dapr (one-time setup)
dapr init

# Start Dapr sidecar for testing (uses default port 4001) in another terminal
# It uses a redis statestore for the workflow db. This is usually installed when doing `dapr init`
dapr run \
  --app-id test-app \
  --dapr-grpc-port 4001 \
  --resources-path ./examples/components


```bash
# Install tox if not already installed
pip install tox

# Install dependencies
pip install -r dev-requirements.txt

# Run unit tests with tox (recommended - uses clean environments)
tox -e py310

# Run E2E tests with tox (requires sidecar - see setup below)
tox -e py310-e2e

# Test with specific Python version
tox -e py311  # or py312, py313, etc.

# Run tests with coverage
tox -e py310
coverage report
```

## Test Categories

### Unit Tests
- **No external dependencies** - Run without sidecar
- **Fast execution** - Suitable for development and CI
- **Isolated testing** - Mock external dependencies

```bash
# Run unit tests with tox (recommended)
tox -e py310

# Or test multiple Python versions
tox -e py310,py311,py312
```

### End-to-End (E2E) Tests
- **Require sidecar** - Need running Dapr sidecar
- **Full integration** - Test complete workflow execution
- **Slower execution** - Real network calls and orchestration

```bash
# Run E2E tests with tox (requires sidecar setup)
tox -e py310-e2e

# Or test multiple Python versions
tox -e py310-e2e,py311-e2e
```

## Sidecar Setup for E2E Tests

E2E tests require a running Dapr sidecar. The SDK connects to port **4001** by default.

### Dapr Sidecar Setup (Recommended)

```bash
# Install Dapr CLI (if not already installed)
curl -fsSL https://raw.githubusercontent.com/dapr/cli/master/install/install.sh | /bin/bash

# Initialize Dapr (one-time setup)
dapr init

# Start Dapr sidecar for testing (uses default port 4001)
# It uses a redis statestore for the workflow db. This is usually installed when doing `dapr init`
dapr run \
  --app-id test-app \
  --dapr-grpc-port 4001 \
  --resources-path ./examples/components


**Why Dapr:**
- **Production parity**: Same runtime as deployed applications
- **Full Dapr features**: State stores, pub/sub, bindings, etc.
- **Real workflow backend**: Actual Dapr workflow engine
- **Debugging**: Same logging and tracing as production

## Configuration

### Environment Variables

The SDK connects to **localhost:4001** by default. Override for non-default configurations:

```bash
# Connect to custom endpoint (format: host:port)
export DAPR_GRPC_ENDPOINT=localhost:50001


### Test-Specific Configuration

```bash
# Enable debug logging for tests
export DAPR_WF_DEBUG=true
export DT_DEBUG=true

# Disable non-determinism detection globally
export DAPR_WF_DISABLE_DETERMINISTIC_DETECTION=true
```

## Running Specific Test Suites

### Core Functionality Tests
```bash
# Run all unit tests (recommended)
tox -e py310

# Run specific test file (use pytest directly if needed)
python -m pytest tests/durabletask/test_orchestration_executor.py -v

# Run specific test case in test file
python -m pytest tests/durabletask/test_orchestration_executor.py::test_fan_in -v

# Run specific test pattern
python -m pytest -k "orchestration" -v
```

### Async Workflow Tests
```bash
# Run async-specific tests
python -m pytest tests/aio -v

# Run determinism tests
python -m pytest tests/durabletask/test_deterministic.py -v
```

### End-to-End Tests
```bash
# Full E2E test suite with tox (requires sidecar on port 4001)
tox -e py310-e2e

# Run with custom endpoint
DAPR_GRPC_ENDPOINT=localhost:50001 tox -e py310-e2e

# Run specific E2E test
python -m pytest tests/durabletask/test_orchestration_e2e.py -v
```


## Test Development Guidelines

### Writing Unit Tests

1. **Use mocks** for external dependencies
2. **Test edge cases** and error conditions
3. **Keep tests fast** and isolated
4. **Use descriptive test names** that explain the scenario

```python
def test_async_workflow_context_timeout_with_cancellation():
    """Test that timeout properly cancels ongoing operations."""
    # Test implementation
```

### Writing E2E Tests

1. **Mark with `@pytest.mark.e2e`** decorator
2. **Use unique orchestration names** to avoid conflicts
3. **Clean up resources** in test teardown
4. **Test realistic scenarios** end-to-end

```python
@pytest.mark.e2e
def test_complex_workflow_with_retries():
    """Test complete workflow with retry policies and error handling."""
    # Test implementation
```

### Enhanced Async Tests

1. **Test both sync and async paths** when applicable
2. **Verify determinism** in replay scenarios
3. **Test sandbox modes** (`off`, `best_effort`, `strict`)
4. **Include performance considerations**

```python
def test_sandbox_mode_performance_impact():
    """Verify sandbox modes have expected performance characteristics."""
    # Test implementation
```

## Debugging Tests

### Enable Debug Logging

```bash
# Enable comprehensive debug logging
export DAPR_WF_DEBUG=true
export DT_DEBUG=true

# Run tests with verbose output using tox
tox -e py310 -- -v -s

# Or run specific test directly with pytest
python -m pytest tests/durabletask/test_deterministic.py -v -s
```

### Debug Specific Features

```bash
# Debug specific test with pytest
python -m pytest tests/aio/test_context.py::TestAsyncWorkflowContext::test_deterministic_uuid -v -s

# Debug with tox and custom pytest args
tox -e py310 -- tests/aio/test_context.py -v -s
```

### Common Issues and Solutions

#### Connection Issues
```bash
# Check if Dapr sidecar is running
dapr list

# Verify port 4001 is listening
lsof -i :4001

# Test with custom endpoint
DAPR_GRPC_ENDPOINT=localhost:50001 tox -e py310-e2e
```

#### Import Issues
```bash
# Install in development mode
pip install -e .

# Verify installation
python -c "import durabletask; print(durabletask.__file__)"
```


### Local CI Simulation

```bash
# Simulate CI environment locally with tox
pip install tox

# Start Dapr sidecar
dapr run \
  --app-id test-app \
  --dapr-grpc-port 4001 \
  --log-level debug &
sleep 10

# Run tests with tox
tox -e py310
tox -e py310-e2e
```

## Performance Testing

### Benchmarking

```bash
# Run performance-sensitive tests with tox
tox -e py310 -- tests/aio -v

# Profile test execution
python -m cProfile -o profile.stats -m pytest tests/aio -v
```

### Load Testing

```bash
# Run concurrency tests with tox
tox -e py310 -- tests/durabletask/test_worker_concurrency_loop.py -v
```

## Contributing Guidelines

### Before Submitting Tests

1. **Run the full test suite**:
   ```bash
   tox -e py310
   tox -e py310-e2e  # with sidecar running
   ```

2. **Check code formatting and linting**:
   ```bash
   tox -e ruff
   ```

3. **Test with multiple Python versions**:
   ```bash
   tox -e py310,py311,py312
   ```

### Test Coverage

Maintain high test coverage for new features:

```bash
# Generate coverage report
tox -e py310
coverage report

# Generate HTML coverage report
coverage html
open htmlcov/index.html
```

### Test Organization

- **Unit tests**: `test_*.py` files without `@pytest.mark.e2e`
- **E2E tests**: `test_*_e2e.py` files or tests marked with `@pytest.mark.e2e`
- **Feature tests**: Group related functionality under `tests/aio/`
- **Integration tests**: Test interactions between components

### Documentation

- **Document complex test scenarios** with clear comments
- **Include setup/teardown requirements** in test docstrings
- **Explain non-obvious test assertions**
- **Update this README** when adding new test categories

## Troubleshooting

### Common Test Failures

1. **Connection refused**: Sidecar not running or wrong port
2. **Timeout errors**: Increase timeout or check sidecar performance
3. **Import errors**: Run `pip install -e .` to install in development mode
4. **Flaky tests**: Check for race conditions or resource cleanup issues

### Getting Help

- **Check existing issues** in the repository
- **Run tests with `-v -s`** for detailed output
- **Enable debug logging** with environment variables
- **Isolate failing tests** by running them individually

### Reporting Issues

When reporting test failures, include:

1. **Python version**: `python --version`
2. **Test command**: Exact command that failed
3. **Environment variables**: Relevant configuration
4. **Sidecar setup**: How the sidecar was started
5. **Full error output**: Complete traceback and logs

## Additional Resources

- [Main README](../README.md) - General SDK documentation
- [ASYNC_ENHANCEMENTS.md](../ASYNC_ENHANCEMENTS.md) - Enhanced async features
- [Examples](../examples/) - Working code samples
- [Makefile](../Makefile) - Build and test commands
- [tox.ini](../tox.ini) - Multi-environment testing configuration
