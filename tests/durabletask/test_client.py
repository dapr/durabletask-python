from unittest.mock import patch

from durabletask.internal.grpc_interceptor import DefaultClientInterceptorImpl
from durabletask.internal.shared import get_default_host_address, get_grpc_channel

HOST_ADDRESS = "localhost:50051"
METADATA = [("key1", "value1"), ("key2", "value2")]
INTERCEPTORS = [DefaultClientInterceptorImpl(METADATA)]


def test_get_grpc_channel_insecure():
    with patch("grpc.insecure_channel") as mock_channel:
        get_grpc_channel(HOST_ADDRESS, False, interceptors=INTERCEPTORS)
        args, kwargs = mock_channel.call_args
        assert args[0] == HOST_ADDRESS
        assert "options" in kwargs and kwargs["options"] is None


def test_get_grpc_channel_secure():
    with (
        patch("grpc.secure_channel") as mock_channel,
        patch("grpc.ssl_channel_credentials") as mock_credentials,
    ):
        get_grpc_channel(HOST_ADDRESS, True, interceptors=INTERCEPTORS)
        args, kwargs = mock_channel.call_args
        assert args[0] == HOST_ADDRESS
        assert args[1] == mock_credentials.return_value
        assert "options" in kwargs and kwargs["options"] is None


def test_get_grpc_channel_default_host_address():
    with patch("grpc.insecure_channel") as mock_channel:
        get_grpc_channel(None, False, interceptors=INTERCEPTORS)
        args, kwargs = mock_channel.call_args
        assert args[0] == get_default_host_address()
        assert "options" in kwargs and kwargs["options"] is None


def test_get_grpc_channel_with_metadata():
    with (
        patch("grpc.insecure_channel") as mock_channel,
        patch("grpc.intercept_channel") as mock_intercept_channel,
    ):
        get_grpc_channel(HOST_ADDRESS, False, interceptors=INTERCEPTORS)
        args, kwargs = mock_channel.call_args
        assert args[0] == HOST_ADDRESS
        assert "options" in kwargs and kwargs["options"] is None
        mock_intercept_channel.assert_called_once()

        # Capture and check the arguments passed to intercept_channel()
        args, kwargs = mock_intercept_channel.call_args
        assert args[0] == mock_channel.return_value
        assert isinstance(args[1], DefaultClientInterceptorImpl)
        assert args[1]._metadata == METADATA


def test_grpc_channel_with_host_name_protocol_stripping():
    with (
        patch("grpc.insecure_channel") as mock_insecure_channel,
        patch("grpc.secure_channel") as mock_secure_channel,
    ):
        host_name = "myserver.com:1234"

        prefix = "grpc://"
        get_grpc_channel(prefix + host_name, interceptors=INTERCEPTORS)
        args, kwargs = mock_insecure_channel.call_args
        assert args[0] == host_name
        assert "options" in kwargs and kwargs["options"] is None

        prefix = "http://"
        get_grpc_channel(prefix + host_name, interceptors=INTERCEPTORS)
        args, kwargs = mock_insecure_channel.call_args
        assert args[0] == host_name
        assert "options" in kwargs and kwargs["options"] is None

        prefix = "HTTP://"
        get_grpc_channel(prefix + host_name, interceptors=INTERCEPTORS)
        args, kwargs = mock_insecure_channel.call_args
        assert args[0] == host_name
        assert "options" in kwargs and kwargs["options"] is None

        prefix = "GRPC://"
        get_grpc_channel(prefix + host_name, interceptors=INTERCEPTORS)
        args, kwargs = mock_insecure_channel.call_args
        assert args[0] == host_name
        assert "options" in kwargs and kwargs["options"] is None

        prefix = ""
        get_grpc_channel(prefix + host_name, interceptors=INTERCEPTORS)
        args, kwargs = mock_insecure_channel.call_args
        assert args[0] == host_name
        assert "options" in kwargs and kwargs["options"] is None

        prefix = "grpcs://"
        get_grpc_channel(prefix + host_name, interceptors=INTERCEPTORS)
        args, kwargs = mock_secure_channel.call_args
        assert args[0] == host_name
        assert "options" in kwargs and kwargs["options"] is None

        prefix = "https://"
        get_grpc_channel(prefix + host_name, interceptors=INTERCEPTORS)
        args, kwargs = mock_secure_channel.call_args
        assert args[0] == host_name
        assert "options" in kwargs and kwargs["options"] is None

        prefix = "HTTPS://"
        get_grpc_channel(prefix + host_name, interceptors=INTERCEPTORS)
        args, kwargs = mock_secure_channel.call_args
        assert args[0] == host_name
        assert "options" in kwargs and kwargs["options"] is None

        prefix = "GRPCS://"
        get_grpc_channel(prefix + host_name, interceptors=INTERCEPTORS)
        args, kwargs = mock_secure_channel.call_args
        assert args[0] == host_name
        assert "options" in kwargs and kwargs["options"] is None

        prefix = ""
        get_grpc_channel(prefix + host_name, True, interceptors=INTERCEPTORS)
        args, kwargs = mock_secure_channel.call_args
        assert args[0] == host_name
        assert "options" in kwargs and kwargs["options"] is None
