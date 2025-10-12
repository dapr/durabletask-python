# Testing Guide

This directory contains comprehensive tests for the durabletask-python SDK, including both unit tests and end-to-end (E2E) tests.

## Quick Start

```bash
# Install dependencies
pip install -r dev-requirements.txt

# Run all unit tests (no sidecar required)
make test-unit

# Run E2E tests (requires sidecar - see setup below)
make test-e2e

# Run specific test file
pytest tests/durabletask/test_async_orchestrator.py -v

# Run tests with coverage
pytest --cov=durabletask --cov-report=html
```

## Test Categories

### Unit Tests
- **No external dependencies** - Run without sidecar
- **Fast execution** - Suitable for development and CI
- **Isolated testing** - Mock external dependencies

```bash
# Run only unit tests
pytest -m "not e2e" --verbose
```

### End-to-End (E2E) Tests
- **Require sidecar** - Need running DurableTask sidecar
- **Full integration** - Test complete workflow execution
- **Slower execution** - Real network calls and orchestration

```bash
# Run only E2E tests (requires sidecar setup)
pytest -m e2e --verbose
```

## Sidecar Setup for E2E Tests

E2E tests require a running DurableTask-compatible sidecar. Since you'll ultimately be deploying to Dapr, **we recommend using Dapr sidecar for development** to match your production environment.

### Option 1: Dapr Sidecar (Recommended for Production Parity)

```bash
# Install Dapr CLI (if not already installed)
curl -fsSL https://raw.githubusercontent.com/dapr/cli/master/install/install.sh | /bin/bash

# Initialize Dapr (one-time setup)
dapr init

# Start Dapr sidecar for testing
dapr run \
  --app-id durabletask-test \
  --dapr-grpc-port 50001 \
  --dapr-http-port 3500 \
  --log-level debug \
  --components-path ./dapr-components \
  -- sleep 3600

# Alternative: Minimal setup without components
dapr run \
  --app-id durabletask-test \
  --dapr-grpc-port 50001 \
  --log-level debug \
  -- sleep 3600
```

**Advantages:**
- **Production parity**: Same runtime as your deployed applications
- **Full Dapr features**: Access to state stores, pub/sub, bindings, etc.
- **Real workflow backend**: Uses actual Dapr workflow engine
- **Debugging**: Same logging and tracing as production

### Option 2: DurableTask-Go Emulator (Lightweight Alternative)

```bash
# Install DurableTask-Go
go install github.com/dapr/durabletask-go@main

# Start the emulator (default port 4001)
durabletask-go --port 4001
```

**Use when:**
- Quick testing without full Dapr setup
- CI/CD environments where speed matters
- Minimal dependencies preferred

### Option 3: Docker Dapr Sidecar

```bash
# Run Dapr sidecar in Docker
docker run --rm -d \
  --name dapr-sidecar \
  -p 50001:50001 \
  -p 3500:3500 \
  daprio/daprd:latest \
  ./daprd \
  --app-id durabletask-test \
  --dapr-grpc-port 50001 \
  --dapr-http-port 3500 \
  --log-level debug
```

## Configuration

### Environment Variables

Configure the SDK connection using these environment variables (checked in order):

```bash
# For Dapr sidecar (recommended - matches production)
export DURABLETASK_GRPC_ENDPOINT=localhost:50001

# For DurableTask-Go emulator (lightweight testing)
export DURABLETASK_GRPC_ENDPOINT=localhost:4001

# Alternative: Host and port separately
export DURABLETASK_GRPC_HOST=localhost
export DURABLETASK_GRPC_PORT=50001  # Dapr default

# Legacy configuration
export TASKHUB_GRPC_ENDPOINT=localhost:50001
```

### Test-Specific Configuration

```bash
# Enable debug logging for tests
export DAPR_WF_DEBUG=true
export DT_DEBUG=true

# Disable non-determinism detection globally
export DAPR_WF_DISABLE_DETECTION=true

# Custom test timeout
export TEST_TIMEOUT=60
```

## Running Specific Test Suites

### Core Functionality Tests
```bash
# Basic orchestration and activity tests
pytest tests/durabletask/test_orchestration_executor.py -v

# Client API tests
pytest tests/durabletask/test_client.py -v

# Worker concurrency tests
pytest tests/durabletask/test_worker_concurrency_loop.py -v
```

### Async Workflow Tests
```bash
# Enhanced async features
pytest tests/aio -v

# Non-determinism detection
pytest tests/durabletask/test_non_determinism_detection.py -v

# Basic async orchestrator tests
pytest tests/durabletask/test_async_orchestrator.py -v
```

### End-to-End Tests
```bash
# Full E2E test suite (requires sidecar)
pytest -q -k "e2e"

# Run with Dapr sidecar (recommended)
DURABLETASK_GRPC_ENDPOINT=localhost:50001 pytest -q -k "e2e"

# Run with DurableTask-Go emulator
DURABLETASK_GRPC_ENDPOINT=localhost:4001 pytest -q -k "e2e"
```

## Dapr-Specific Testing

### Dapr Components Setup

For advanced testing with Dapr features, create a `dapr-components` directory:

```bash
mkdir -p dapr-components

# Example: State store component
cat > dapr-components/statestore.yaml << EOF
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: statestore
spec:
  type: state.in-memory
  version: v1
EOF

# Example: Pub/Sub component  
cat > dapr-components/pubsub.yaml << EOF
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: pubsub
spec:
  type: pubsub.in-memory
  version: v1
EOF
```

### Dapr Workflow Testing

```bash
# Start Dapr with workflow support
dapr run \
  --app-id workflow-test \
  --dapr-grpc-port 50001 \
  --enable-api-logging \
  --log-level debug \
  --components-path ./dapr-components \
  -- sleep 3600

# Run tests against Dapr
export DURABLETASK_GRPC_ENDPOINT=localhost:50001
pytest tests/durabletask/test_orchestration_e2e.py -v
```

### Production-Like Testing

```bash
# Test with Dapr + Redis (closer to production)
docker run -d --name redis -p 6379:6379 redis:alpine

cat > dapr-components/redis-statestore.yaml << EOF
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: statestore
spec:
  type: state.redis
  version: v1
  metadata:
  - name: redisHost
    value: localhost:6379
  - name: redisPassword
    value: ""
EOF

# Start Dapr with Redis backend
dapr run \
  --app-id workflow-prod-test \
  --dapr-grpc-port 50001 \
  --components-path ./dapr-components \
  -- sleep 3600
```

### Dapr Debugging

```bash
# Enable detailed Dapr logging
dapr run \
  --app-id workflow-debug \
  --dapr-grpc-port 50001 \
  --log-level debug \
  --enable-api-logging \
  --enable-metrics \
  --metrics-port 9090 \
  -- sleep 3600

# View Dapr dashboard (optional)
dapr dashboard
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

# Run tests with verbose output
pytest tests/durabletask/test_async_orchestrator.py -v -s
```

### Debug Specific Features

```bash
# Debug non-determinism detection
pytest tests/durabletask/test_non_determinism_detection.py::test_strict_mode_raises_error -v -s

# Debug specific enhanced features
pytest tests/aio/test_context.py::TestAsyncWorkflowContext::test_debug_mode_detection -v -s
```

### Common Issues and Solutions

#### Connection Issues
```bash
# Check if sidecar is running
curl -f http://localhost:4001/health || echo "Sidecar not responding"

# Test with different endpoint
DURABLETASK_GRPC_ENDPOINT=localhost:50001 pytest -m e2e
```

#### Timeout Issues
```bash
# Increase test timeouts
pytest --timeout=120 tests/durabletask/test_orchestration_e2e.py
```

#### Import Issues
```bash
# Install in development mode
pip install -e .

# Verify installation
python -c "import durabletask; print(durabletask.__file__)"
```

## Continuous Integration

### GitHub Actions Setup

```yaml
# Example CI configuration with Dapr
- name: Run Unit Tests
  run: |
    pip install -r dev-requirements.txt
    make test-unit

- name: Setup Dapr
  uses: dapr/setup-dapr@v1
  with:
    version: '1.12.0'

- name: Start Dapr Sidecar
  run: |
    dapr run \
      --app-id ci-test \
      --dapr-grpc-port 50001 \
      --log-level debug \
      -- sleep 300 &
    sleep 10

- name: Run E2E Tests
  run: make test-e2e
  env:
    DURABLETASK_GRPC_ENDPOINT: localhost:50001

# Alternative: Lightweight CI with DurableTask-Go
- name: Start DurableTask Emulator
  run: |
    go install github.com/dapr/durabletask-go@main
    durabletask-go --port 4001 &
    sleep 5

- name: Run E2E Tests (Lightweight)
  run: make test-e2e
  env:
    DURABLETASK_GRPC_ENDPOINT: localhost:4001
```

### Local CI Simulation

```bash
# Simulate CI environment locally with Dapr (recommended)
dapr run \
  --app-id local-ci-test \
  --dapr-grpc-port 50001 \
  --log-level debug \
  -- sleep 300 &
sleep 10
export DURABLETASK_GRPC_ENDPOINT=localhost:50001
make test-unit
make test-e2e

# Alternative: Lightweight simulation
export DURABLETASK_GRPC_ENDPOINT=localhost:4001
durabletask-go --port 4001 &
sleep 5
make test-unit
make test-e2e
```

## Performance Testing

### Benchmarking

```bash
# Run performance-sensitive tests
pytest tests/aio/test_awaitables.py::TestAwaitables::test_slots_memory_optimization -v

# Profile test execution
python -m cProfile -o profile.stats -m pytest tests/durabletask/test_async_orchestrator.py
```

### Load Testing

```bash
# Run concurrency tests
pytest tests/durabletask/test_worker_concurrency_loop.py -v
pytest tests/durabletask/test_worker_concurrency_loop_async.py -v
```

## Contributing Guidelines

### Before Submitting Tests

1. **Run the full test suite**:
   ```bash
   make test-unit
   make test-e2e  # with sidecar running
   ```

2. **Check code formatting**:
   ```bash
   ruff format
   flake8 .
   ```

3. **Verify type annotations**:
   ```bash
   mypy --config-file mypy.ini
   ```

4. **Test with multiple Python versions** (if available):
   ```bash
   tox -e py39,py310,py311,py312
   ```

### Test Coverage

Maintain high test coverage for new features:

```bash
# Generate coverage report
pytest --cov=durabletask --cov-report=html --cov-report=term

# View coverage in browser
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
