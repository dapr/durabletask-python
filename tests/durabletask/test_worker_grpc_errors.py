"""
Copyright 2025 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from unittest.mock import MagicMock, Mock, patch

import grpc

from durabletask import worker


def test_execute_orchestrator_grpc_error_benign_cancelled():
    """Test that benign gRPC errors in orchestrator execution are handled gracefully."""
    w = worker.TaskHubGrpcWorker()

    # Add a dummy orchestrator
    def test_orchestrator(ctx, input):
        return "result"

    w.add_orchestrator(test_orchestrator)

    # Mock the stub to raise a benign error
    mock_stub = MagicMock()
    mock_error = grpc.RpcError()
    mock_error.code = Mock(return_value=grpc.StatusCode.CANCELLED)
    mock_stub.CompleteOrchestratorTask.side_effect = mock_error

    # Create a mock request with proper structure
    mock_req = MagicMock()
    mock_req.instanceId = "test-id"
    mock_req.pastEvents = []
    mock_req.newEvents = [MagicMock()]
    mock_req.newEvents[0].HasField = lambda x: x == "executionStarted"
    mock_req.newEvents[0].executionStarted.name = "test_orchestrator"
    mock_req.newEvents[0].executionStarted.input = None
    mock_req.newEvents[0].router.targetAppID = None
    mock_req.newEvents[0].router.sourceAppID = None
    mock_req.newEvents[0].timestamp.ToDatetime = Mock(return_value=None)

    # Should not raise exception (benign error)
    w._execute_orchestrator(mock_req, mock_stub, "token")


def test_execute_orchestrator_grpc_error_non_benign():
    """Test that non-benign gRPC errors in orchestrator execution are logged."""
    w = worker.TaskHubGrpcWorker()

    # Add a dummy orchestrator
    def test_orchestrator(ctx, input):
        return "result"

    w.add_orchestrator(test_orchestrator)

    # Mock the stub to raise a non-benign error
    mock_stub = MagicMock()
    mock_error = grpc.RpcError()
    mock_error.code = Mock(return_value=grpc.StatusCode.INTERNAL)
    mock_stub.CompleteOrchestratorTask.side_effect = mock_error

    # Create a mock request with proper structure
    mock_req = MagicMock()
    mock_req.instanceId = "test-id"
    mock_req.pastEvents = []
    mock_req.newEvents = [MagicMock()]
    mock_req.newEvents[0].HasField = lambda x: x == "executionStarted"
    mock_req.newEvents[0].executionStarted.name = "test_orchestrator"
    mock_req.newEvents[0].executionStarted.input = None
    mock_req.newEvents[0].router.targetAppID = None
    mock_req.newEvents[0].router.sourceAppID = None
    mock_req.newEvents[0].timestamp.ToDatetime = Mock(return_value=None)

    # Should not raise exception (error is logged but handled)
    with patch.object(w._logger, "exception") as mock_log:
        w._execute_orchestrator(mock_req, mock_stub, "token")
        # Verify error was logged
        assert mock_log.called


def test_execute_activity_grpc_error_benign():
    """Test that benign gRPC errors in activity execution are handled gracefully."""
    w = worker.TaskHubGrpcWorker()

    # Add a dummy activity
    def test_activity(ctx, input):
        return "result"

    w.add_activity(test_activity)

    # Mock the stub to raise a benign error
    mock_stub = MagicMock()
    mock_error = grpc.RpcError()
    mock_error.code = Mock(return_value=grpc.StatusCode.CANCELLED)
    str_return = "unknown instance ID/task ID combo"
    mock_error.__str__ = Mock(return_value=str_return)
    mock_stub.CompleteActivityTask.side_effect = mock_error

    # Create a mock request
    mock_req = MagicMock()
    mock_req.orchestrationInstance.instanceId = "test-id"
    mock_req.name = "test_activity"
    mock_req.taskId = 1
    mock_req.input.value = '""'

    # Should not raise exception (benign error)
    w._execute_activity(mock_req, mock_stub, "token")
