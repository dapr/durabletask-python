from unittest.mock import patch, ANY, Mock

from durabletask.client import TaskHubGrpcClient
from durabletask.internal.shared import (DefaultClientInterceptorImpl,
                                         get_default_host_address,
                                         get_grpc_channel)
import pytest

HOST_ADDRESS = 'localhost:50051'
METADATA = [('key1', 'value1'), ('key2', 'value2')]


def test_get_grpc_channel_insecure():
    with patch('grpc.insecure_channel') as mock_channel:
        get_grpc_channel(HOST_ADDRESS, METADATA, False)
        mock_channel.assert_called_once_with(HOST_ADDRESS)


def test_get_grpc_channel_secure():
    with patch('grpc.secure_channel') as mock_channel, patch(
            'grpc.ssl_channel_credentials') as mock_credentials:
        get_grpc_channel(HOST_ADDRESS, METADATA, True)
        mock_channel.assert_called_once_with(HOST_ADDRESS, mock_credentials.return_value)


def test_get_grpc_channel_default_host_address():
    with patch('grpc.insecure_channel') as mock_channel:
        get_grpc_channel(None, METADATA, False)
        mock_channel.assert_called_once_with(get_default_host_address())


def test_get_grpc_channel_with_metadata():
    with patch('grpc.insecure_channel') as mock_channel, patch(
            'grpc.intercept_channel') as mock_intercept_channel:
        get_grpc_channel(HOST_ADDRESS, METADATA, False)
        mock_channel.assert_called_once_with(HOST_ADDRESS)
        mock_intercept_channel.assert_called_once()

        # Capture and check the arguments passed to intercept_channel()
        args, kwargs = mock_intercept_channel.call_args
        assert args[0] == mock_channel.return_value
        assert isinstance(args[1], DefaultClientInterceptorImpl)
        assert args[1]._metadata == METADATA


def test_grpc_channel_with_host_name_protocol_stripping():
    with patch('grpc.insecure_channel') as mock_insecure_channel, patch(
            'grpc.secure_channel') as mock_secure_channel:

        host_name = "myserver.com:1234"

        prefix = "grpc://"
        get_grpc_channel(prefix + host_name, METADATA)
        mock_insecure_channel.assert_called_with(host_name)

        prefix = "http://"
        get_grpc_channel(prefix + host_name, METADATA)
        mock_insecure_channel.assert_called_with(host_name)

        prefix = "HTTP://"
        get_grpc_channel(prefix + host_name, METADATA)
        mock_insecure_channel.assert_called_with(host_name)

        prefix = "GRPC://"
        get_grpc_channel(prefix + host_name, METADATA)
        mock_insecure_channel.assert_called_with(host_name)

        prefix = ""
        get_grpc_channel(prefix + host_name, METADATA)
        mock_insecure_channel.assert_called_with(host_name)

        prefix = "grpcs://"
        get_grpc_channel(prefix + host_name, METADATA)
        mock_secure_channel.assert_called_with(host_name, ANY)

        prefix = "https://"
        get_grpc_channel(prefix + host_name, METADATA)
        mock_secure_channel.assert_called_with(host_name, ANY)

        prefix = "HTTPS://"
        get_grpc_channel(prefix + host_name, METADATA)
        mock_secure_channel.assert_called_with(host_name, ANY)

        prefix = "GRPCS://"
        get_grpc_channel(prefix + host_name, METADATA)
        mock_secure_channel.assert_called_with(host_name, ANY)

        prefix = ""
        get_grpc_channel(prefix + host_name, METADATA, True)
        mock_secure_channel.assert_called_with(host_name, ANY)


@pytest.mark.parametrize("timeout", [None, 0, 5])
def test_wait_for_orchestration_start_timeout(timeout):
    instance_id = "test-instance"

    from durabletask.internal.orchestrator_service_pb2 import GetInstanceResponse, \
        OrchestrationState, ORCHESTRATION_STATUS_RUNNING

    response = GetInstanceResponse()
    state = OrchestrationState()
    state.instanceId = instance_id
    state.orchestrationStatus = ORCHESTRATION_STATUS_RUNNING
    response.orchestrationState.CopyFrom(state)

    c = TaskHubGrpcClient()
    c._stub = Mock()
    c._stub.WaitForInstanceStart.return_value = response

    grpc_timeout = None if timeout is None else timeout
    c.wait_for_orchestration_start(instance_id, timeout=grpc_timeout)

    # Verify WaitForInstanceStart was called with timeout=None
    c._stub.WaitForInstanceStart.assert_called_once()
    _, kwargs = c._stub.WaitForInstanceStart.call_args
    if timeout is None or timeout == 0:
        assert kwargs.get('timeout') is None
    else:
        assert kwargs.get('timeout') == timeout

@pytest.mark.parametrize("timeout", [None, 0, 5])
def test_wait_for_orchestration_completion_timeout(timeout):
    instance_id = "test-instance"

    from durabletask.internal.orchestrator_service_pb2 import GetInstanceResponse, \
        OrchestrationState, ORCHESTRATION_STATUS_COMPLETED

    response = GetInstanceResponse()
    state = OrchestrationState()
    state.instanceId = instance_id
    state.orchestrationStatus = ORCHESTRATION_STATUS_COMPLETED
    response.orchestrationState.CopyFrom(state)

    c = TaskHubGrpcClient()
    c._stub = Mock()
    c._stub.WaitForInstanceCompletion.return_value = response

    grpc_timeout = None if timeout is None else timeout
    c.wait_for_orchestration_completion(instance_id, timeout=grpc_timeout)

    # Verify WaitForInstanceStart was called with timeout=None
    c._stub.WaitForInstanceCompletion.assert_called_once()
    _, kwargs = c._stub.WaitForInstanceCompletion.call_args
    if timeout is None or timeout == 0:
        assert kwargs.get('timeout') is None
    else:
        assert kwargs.get('timeout') == timeout