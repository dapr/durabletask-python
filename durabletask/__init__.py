# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Durable Task SDK for Python"""


PACKAGE_NAME = "durabletask"

# Public async compatibility exports
from durabletask.asyncio_compat import (  # noqa: F401
    AsyncWorkflowContext,
    CoroutineOrchestratorRunner,
)
