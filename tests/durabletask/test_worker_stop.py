from unittest.mock import MagicMock, patch

import grpc

from durabletask.worker import TaskHubGrpcWorker


# Helper to create a running worker with a mocked runLoop
def _make_running_worker():
    worker = TaskHubGrpcWorker()
    worker._is_running = True
    worker._runLoop = MagicMock()
    worker._runLoop.is_alive.return_value = False
    return worker


def test_stop_with_grpc_future():
    worker = _make_running_worker()
    mock_future = MagicMock(spec=grpc.Future)
    worker._response_stream = mock_future
    worker.stop()
    mock_future.cancel.assert_called_once()


def test_stop_with_generator_call():
    worker = _make_running_worker()
    mock_call = MagicMock()
    mock_stream = MagicMock()
    mock_stream.call = mock_call
    worker._response_stream = mock_stream
    worker.stop()
    mock_call.cancel.assert_called_once()


def test_stop_with_unknown_stream_type(caplog):
    worker = _make_running_worker()
    # Not a grpc.Future, no 'call' attribute
    worker._response_stream = object()
    with caplog.at_level("WARNING"):
        worker.stop()
    assert any("Error cancelling response stream: " in m for m in caplog.text.splitlines())


def test_stop_with_none_stream():
    worker = _make_running_worker()
    worker._response_stream = None
    # Should not raise
    worker.stop()


def test_stop_when_not_running():
    worker = TaskHubGrpcWorker()
    worker._is_running = False
    # Should return immediately, not set _shutdown
    with patch.object(worker._shutdown, "set") as shutdown_set:
        worker.stop()
        shutdown_set.assert_not_called()
