# Copyright (c) The Dapr Authors.
# Licensed under the MIT License.

from typing import Any, Optional, Sequence, Union

import grpc
from grpc import aio as grpc_aio

from durabletask.internal.shared import (
    INSECURE_PROTOCOLS,
    SECURE_PROTOCOLS,
    get_default_host_address,
)

ClientInterceptor = Union[
    grpc_aio.UnaryUnaryClientInterceptor,
    grpc_aio.UnaryStreamClientInterceptor,
    grpc_aio.StreamUnaryClientInterceptor,
    grpc_aio.StreamStreamClientInterceptor,
]


def get_grpc_aio_channel(
    host_address: Optional[str],
    secure_channel: bool = False,
    interceptors: Optional[Sequence[ClientInterceptor]] = None,
    options: Optional[Sequence[tuple[str, Any]]] = None,
) -> grpc_aio.Channel:
    """create a grpc asyncio channel

    Args:
        host_address: The host address of the gRPC server. If None, uses the default address.
        secure_channel: Whether to use a secure channel (TLS/SSL). Defaults to False.
        interceptors: Optional sequence of client interceptors to apply to the channel.
        options: Optional sequence of gRPC channel options as (key, value) tuples. Keys defined in https://grpc.github.io/grpc/core/group__grpc__arg__keys.html
    """
    if host_address is None:
        host_address = get_default_host_address()

    for protocol in SECURE_PROTOCOLS:
        if host_address.lower().startswith(protocol):
            secure_channel = True
            host_address = host_address[len(protocol) :]
            break

    for protocol in INSECURE_PROTOCOLS:
        if host_address.lower().startswith(protocol):
            secure_channel = False
            host_address = host_address[len(protocol) :]
            break

    # channel interceptors/options
    channel_kwargs = dict(interceptors=interceptors)
    if options is not None:
        # validate all options keys prefix starts with `grpc.`
        if not all(key.startswith('grpc.') for key, _ in options):
            raise ValueError(
                f'All options keys must start with `grpc.`. Invalid options: {options}'
            )
        channel_kwargs["options"] = options

    if secure_channel:
        channel = grpc_aio.secure_channel(
            host_address, grpc.ssl_channel_credentials(), **channel_kwargs
        )
    else:
        channel = grpc_aio.insecure_channel(host_address, **channel_kwargs)

    return channel
