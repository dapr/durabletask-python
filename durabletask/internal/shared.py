# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import dataclasses
import json
import logging
import os
from types import SimpleNamespace
from typing import Any, Dict, Iterable, Optional, Sequence, Union

import grpc

ClientInterceptor = Union[
    grpc.UnaryUnaryClientInterceptor,
    grpc.UnaryStreamClientInterceptor,
    grpc.StreamUnaryClientInterceptor,
    grpc.StreamStreamClientInterceptor,
]

# Field name used to indicate that an object was automatically serialized
# and should be deserialized as a SimpleNamespace
AUTO_SERIALIZED = "__durabletask_autoobject__"

SECURE_PROTOCOLS = ["https://", "grpcs://"]
INSECURE_PROTOCOLS = ["http://", "grpc://"]


def get_default_host_address() -> str:
    """Resolve the default Durable Task sidecar address.

    Honors environment variables if present; otherwise defaults to localhost:4001.

    Supported environment variables (checked in order):
    - DAPR_GRPC_ENDPOINT (e.g., "localhost:4001")
    - DAPR_GRPC_HOST/DAPR_RUNTIME_HOST and DAPR_GRPC_PORT
    """
    import os

    # Full endpoint overrides
    endpoint = os.environ.get("DAPR_GRPC_ENDPOINT")
    if endpoint:
        return endpoint

    # Host/port split overrides
    host = os.environ.get("DAPR_GRPC_HOST") or os.environ.get("DAPR_RUNTIME_HOST")
    if host:
        port = os.environ.get("DAPR_GRPC_PORT", "4001")
        return f"{host}:{port}"

    # Default to durabletask-go default port
    return "localhost:4001"


def get_grpc_channel(
    host_address: Optional[str],
    secure_channel: bool = False,
    interceptors: Optional[Sequence[ClientInterceptor]] = None,
    options: Optional[Sequence[tuple[str, Any]]] = None,
) -> grpc.Channel:
    if host_address is None:
        host_address = get_default_host_address()

    for protocol in SECURE_PROTOCOLS:
        if host_address.lower().startswith(protocol):
            secure_channel = True
            # remove the protocol from the host name
            host_address = host_address[len(protocol) :]
            break

    for protocol in INSECURE_PROTOCOLS:
        if host_address.lower().startswith(protocol):
            secure_channel = False
            # remove the protocol from the host name
            host_address = host_address[len(protocol) :]
            break

    # Build channel options (merge provided options with env-driven keepalive/retry)
    channel_options = build_grpc_channel_options(options)

    # Create the base channel (preserve original call signature when no options)
    if secure_channel:
        if channel_options is not None:
            channel = grpc.secure_channel(
                host_address, grpc.ssl_channel_credentials(), options=channel_options
            )
        else:
            channel = grpc.secure_channel(host_address, grpc.ssl_channel_credentials())
    else:
        if channel_options is not None:
            channel = grpc.insecure_channel(host_address, options=channel_options)
        else:
            channel = grpc.insecure_channel(host_address)

    # Apply interceptors ONLY if they exist
    if interceptors:
        channel = grpc.intercept_channel(channel, *interceptors)
    return channel


def _get_env_bool(name: str, default: bool) -> bool:
    val = os.environ.get(name)
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "t", "yes", "y"}


def _get_env_int(name: str, default: int) -> int:
    val = os.environ.get(name)
    if val is None:
        return default
    try:
        return int(val)
    except Exception:
        return default


def _get_env_float(name: str, default: float) -> float:
    val = os.environ.get(name)
    if val is None:
        return default
    try:
        return float(val)
    except Exception:
        return default


def _get_env_csv(name: str, default_csv: str) -> list[str]:
    val = os.environ.get(name, default_csv)
    return [s.strip().upper() for s in val.split(",") if s.strip()]


def get_grpc_keepalive_options() -> list[tuple[str, Any]]:
    """Build gRPC keepalive channel options from environment variables.

    Environment variables (defaults in parentheses):
    - DAPR_GRPC_KEEPALIVE_ENABLED (false)
    - DAPR_GRPC_KEEPALIVE_TIME_MS (120000)
    - DAPR_GRPC_KEEPALIVE_TIMEOUT_MS (20000)
    - DAPR_GRPC_KEEPALIVE_PERMIT_WITHOUT_CALLS (false)
    """
    enabled = _get_env_bool("DAPR_GRPC_KEEPALIVE_ENABLED", False)
    if not enabled:
        return []
    time_ms = _get_env_int("DAPR_GRPC_KEEPALIVE_TIME_MS", 120000)
    timeout_ms = _get_env_int("DAPR_GRPC_KEEPALIVE_TIMEOUT_MS", 20000)
    permit_without_calls = (
        1 if _get_env_bool("DAPR_GRPC_KEEPALIVE_PERMIT_WITHOUT_CALLS", False) else 0
    )
    return [
        ("grpc.keepalive_time_ms", time_ms),
        ("grpc.keepalive_timeout_ms", timeout_ms),
        ("grpc.keepalive_permit_without_calls", permit_without_calls),
    ]


def get_grpc_retry_service_config_option() -> Optional[tuple[str, str]]:
    """Return ("grpc.service_config", json_str) if retry is enabled via env; else None.

    Environment variables (defaults in parentheses):
    - DAPR_GRPC_RETRY_ENABLED (false)
    - DAPR_GRPC_RETRY_MAX_ATTEMPTS (4)
    - DAPR_GRPC_RETRY_INITIAL_BACKOFF_MS (100)
    - DAPR_GRPC_RETRY_MAX_BACKOFF_MS (1000)
    - DAPR_GRPC_RETRY_BACKOFF_MULTIPLIER (2.0)
    - DAPR_GRPC_RETRY_CODES (UNAVAILABLE,DEADLINE_EXCEEDED)
    """
    enabled = _get_env_bool("DAPR_GRPC_RETRY_ENABLED", False)
    if not enabled:
        return None

    max_attempts = _get_env_int("DAPR_GRPC_RETRY_MAX_ATTEMPTS", 4)
    initial_backoff_ms = _get_env_int("DAPR_GRPC_RETRY_INITIAL_BACKOFF_MS", 100)
    max_backoff_ms = _get_env_int("DAPR_GRPC_RETRY_MAX_BACKOFF_MS", 1000)
    backoff_multiplier = _get_env_float("DAPR_GRPC_RETRY_BACKOFF_MULTIPLIER", 2.0)
    codes = _get_env_csv("DAPR_GRPC_RETRY_CODES", "UNAVAILABLE,DEADLINE_EXCEEDED")

    service_config = {
        "methodConfig": [
            {
                "name": [{"service": ""}],
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
    return ("grpc.service_config", json.dumps(service_config))


def build_grpc_channel_options(
    base_options: Optional[Iterable[tuple[str, Any]]] = None,
) -> Optional[list[tuple[str, Any]]]:
    """Combine base options + env-driven keepalive and retry service config.

    The returned list is safe to pass as the `options` argument to grpc.secure_channel/insecure_channel.
    """
    combined: list[tuple[str, Any]] = []
    if base_options:
        combined.extend(list(base_options))

    keepalive = get_grpc_keepalive_options()
    if keepalive:
        combined.extend(keepalive)
    retry_opt = get_grpc_retry_service_config_option()
    if retry_opt is not None:
        combined.append(retry_opt)
    return combined if combined else None


def get_logger(
    name_suffix: str,
    log_handler: Optional[logging.Handler] = None,
    log_formatter: Optional[logging.Formatter] = None,
) -> logging.Logger:
    logger = logging.Logger(f"durabletask-{name_suffix}")

    # Add a default log handler if none is provided
    if log_handler is None:
        log_handler = logging.StreamHandler()
        log_handler.setLevel(logging.DEBUG)
    logger.handlers.append(log_handler)

    # Set a default log formatter to our handler if none is provided
    if log_formatter is None:
        log_formatter = logging.Formatter(
            fmt="%(asctime)s.%(msecs)03d %(name)s %(levelname)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    log_handler.setFormatter(log_formatter)
    return logger


def to_json(obj):
    return json.dumps(obj, cls=InternalJSONEncoder)


def from_json(json_str):
    return json.loads(json_str, cls=InternalJSONDecoder)


class InternalJSONEncoder(json.JSONEncoder):
    """JSON encoder that supports serializing specific Python types."""

    def encode(self, obj: Any) -> str:
        # if the object is a namedtuple, convert it to a dict with the AUTO_SERIALIZED key added
        if isinstance(obj, tuple) and hasattr(obj, "_fields") and hasattr(obj, "_asdict"):
            d = obj._asdict()  # type: ignore
            d[AUTO_SERIALIZED] = True
            obj = d
        return super().encode(obj)

    def default(self, obj):
        if dataclasses.is_dataclass(obj):
            # Dataclasses are not serializable by default, so we convert them to a dict and mark them for
            # automatic deserialization by the receiver
            d = dataclasses.asdict(obj)  # type: ignore
            d[AUTO_SERIALIZED] = True
            return d
        elif isinstance(obj, SimpleNamespace):
            # Most commonly used for serializing custom objects that were previously serialized using our encoder
            d = vars(obj)
            d[AUTO_SERIALIZED] = True
            return d
        # This will typically raise a TypeError
        return json.JSONEncoder.default(self, obj)


class InternalJSONDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        super().__init__(object_hook=self.dict_to_object, *args, **kwargs)

    def dict_to_object(self, d: Dict[str, Any]):
        # If the object was serialized by the InternalJSONEncoder, deserialize it as a SimpleNamespace
        if d.pop(AUTO_SERIALIZED, False):
            return SimpleNamespace(**d)
        return d
