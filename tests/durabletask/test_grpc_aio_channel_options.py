import json
from unittest.mock import patch

import pytest

from durabletask.aio.internal.shared import get_grpc_aio_channel

HOST_ADDRESS = "localhost:50051"


def _find_option(options, key):
    for k, v in options:
        if k == key:
            return v
    raise AssertionError(f"Option with key {key} not found in options: {options}")


def test_aio_channel_passes_base_options_and_max_lengths():
    base_options = [
        ("grpc.max_send_message_length", 4321),
        ("grpc.max_receive_message_length", 8765),
        ("grpc.primary_user_agent", "durabletask-aio-tests"),
    ]
    with patch("durabletask.aio.internal.shared.grpc_aio.insecure_channel") as mock_channel:
        get_grpc_aio_channel(HOST_ADDRESS, False, options=base_options)
        # Ensure called with options kwarg
        assert mock_channel.call_count == 1
        args, kwargs = mock_channel.call_args
        assert args[0] == HOST_ADDRESS
        assert "options" in kwargs
        opts = kwargs["options"]
        # Check our base options made it through
        assert ("grpc.max_send_message_length", 4321) in opts
        assert ("grpc.max_receive_message_length", 8765) in opts
        assert ("grpc.primary_user_agent", "durabletask-aio-tests") in opts


def test_aio_channel_merges_env_keepalive_and_retry(monkeypatch: pytest.MonkeyPatch):
    # retry grpc option
    # service_config ref => https://github.com/grpc/grpc-proto/blob/master/grpc/service_config/service_config.proto#L44
    max_attempts = 4
    initial_backoff_ms = 250
    max_backoff_ms = 2000
    backoff_multiplier = 1.5
    codes = ["RESOURCE_EXHAUSTED"]
    service_config = {
        "methodConfig": [
            {
                "name": [{"service": ""}],  # match all services/methods
                "retryPolicy": {
                    "maxAttempts": max_attempts,
                    "initialBackoff": f"{initial_backoff_ms / 1000.0}s",
                    "maxBackoff": f"{max_backoff_ms / 1000.0}s",
                    "backoffMultiplier": backoff_multiplier,
                    "retryableStatusCodes": codes,
                },
            }
        ]
    }

    base_options = [("grpc.service_config", json.dumps(service_config))]

    with patch("durabletask.aio.internal.shared.grpc_aio.insecure_channel") as mock_channel:
        get_grpc_aio_channel(HOST_ADDRESS, False, options=base_options)

        args, kwargs = mock_channel.call_args
        assert args[0] == HOST_ADDRESS
        assert "options" in kwargs
        opts = kwargs["options"]

        # Retry service config present and parses correctly
        svc_cfg_str = _find_option(opts, "grpc.service_config")
        svc_cfg = json.loads(svc_cfg_str)
        assert "methodConfig" in svc_cfg and isinstance(svc_cfg["methodConfig"], list)
        retry_policy = svc_cfg["methodConfig"][0]["retryPolicy"]
        assert retry_policy["maxAttempts"] == 4
        assert retry_policy["initialBackoff"] == f"{250 / 1000.0}s"
        assert retry_policy["maxBackoff"] == f"{2000 / 1000.0}s"
        assert retry_policy["backoffMultiplier"] == 1.5
        # Codes are upper-cased list
        assert "RESOURCE_EXHAUSTED" in retry_policy["retryableStatusCodes"]


def test_aio_secure_channel_receives_options_when_secure_true():
    base_options = [("grpc.max_receive_message_length", 999999)]
    with (
        patch("durabletask.aio.internal.shared.grpc_aio.secure_channel") as mock_channel,
        patch("grpc.ssl_channel_credentials") as mock_credentials,
    ):
        get_grpc_aio_channel(HOST_ADDRESS, True, options=base_options)
        args, kwargs = mock_channel.call_args
        assert args[0] == HOST_ADDRESS
        assert args[1] == mock_credentials.return_value
        assert ("grpc.max_receive_message_length", 999999) in kwargs.get("options", [])
