# Copyright (c) The Dapr Authors.
# Licensed under the MIT License.

import logging
from datetime import datetime
from functools import partial
from typing import Any, Optional, Sequence, Union

from anyio.from_thread import start_blocking_portal  # type: ignore

import durabletask.internal.orchestrator_service_pb2 as pb
import durabletask.internal.shared as shared
from durabletask import task
from durabletask.aio.client import (
    AsyncTaskHubGrpcClient,
    OrchestrationState,
    TInput,
    TOutput,
)


class TaskHubGrpcClient:
    def __init__(
        self,
        *,
        host_address: Optional[str] = None,
        metadata: Optional[list[tuple[str, str]]] = None,
        log_handler: Optional[logging.Handler] = None,
        log_formatter: Optional[logging.Formatter] = None,
        secure_channel: bool = False,
        interceptors: Optional[Sequence[shared.ClientInterceptor]] = None,
        channel_options: Optional[Sequence[tuple[str, Any]]] = None,
    ):
        # Store configuration to construct the async client on demand
        self._host_address = host_address
        self._metadata = metadata
        self._log_handler = log_handler
        self._log_formatter = log_formatter
        self._secure_channel = secure_channel
        self._interceptors = interceptors
        self._channel_options = channel_options

        self._portal_cm = start_blocking_portal()
        self._portal = self._portal_cm.__enter__()
        self._async_client = self._portal.call(self._create_async_client)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def __del__(self):
        # Best-effort cleanup in case the user didn't call close() or use context manager
        try:
            self.close()
        except Exception:
            pass

    async def _create_async_client(self) -> AsyncTaskHubGrpcClient:
        return AsyncTaskHubGrpcClient(
            host_address=self._host_address,
            metadata=self._metadata,
            log_handler=self._log_handler,
            log_formatter=self._log_formatter,
            secure_channel=self._secure_channel,
            interceptors=self._interceptors,
            channel_options=self._channel_options,
        )

    def close(self):
        if self._async_client is not None:
            try:
                self._portal.call(self._async_client.aclose)
            except Exception:
                pass
            self._async_client = None
        if self._portal is not None:
            try:
                self._portal_cm.__exit__(None, None, None)
            except Exception:
                pass
            self._portal = None
            self._portal_cm = None

    def schedule_new_orchestration(
        self,
        orchestrator: Union[task.Orchestrator[TInput, TOutput], str],
        *,
        input: Optional[TInput] = None,
        instance_id: Optional[str] = None,
        start_at: Optional[datetime] = None,
        reuse_id_policy: Optional[pb.OrchestrationIdReusePolicy] = None,
    ) -> str:
        return self._portal.call(  # type: ignore
            partial(
                self._async_client.schedule_new_orchestration,  # type: ignore
                orchestrator,
                input=input,
                instance_id=instance_id,
                start_at=start_at,
                reuse_id_policy=reuse_id_policy,
            )
        )

    def get_orchestration_state(
        self, instance_id: str, *, fetch_payloads: bool = True
    ) -> Optional[OrchestrationState]:
        return self._portal.call(  # type: ignore
            partial(
                self._async_client.get_orchestration_state,  # type: ignore
                instance_id,
                fetch_payloads=fetch_payloads,
            )
        )

    def wait_for_orchestration_start(
        self, instance_id: str, *, fetch_payloads: bool = False, timeout: int = 0
    ) -> Optional[OrchestrationState]:
        return self._portal.call(  # type: ignore
            partial(
                self._async_client.wait_for_orchestration_start,  # type: ignore
                instance_id,
                fetch_payloads=fetch_payloads,
                timeout=timeout,
            )
        )

    def wait_for_orchestration_completion(
        self, instance_id: str, *, fetch_payloads: bool = True, timeout: int = 0
    ) -> Optional[OrchestrationState]:
        return self._portal.call(  # type: ignore
            partial(
                self._async_client.wait_for_orchestration_completion,  # type: ignore
                instance_id,
                fetch_payloads=fetch_payloads,
                timeout=timeout,
            )
        )

    def raise_orchestration_event(
        self, instance_id: str, event_name: str, *, data: Optional[Any] = None
    ):
        self._portal.call(  # type: ignore
            partial(
                self._async_client.raise_orchestration_event,  # type: ignore
                instance_id,
                event_name,
                data=data,
            )
        )

    def terminate_orchestration(
        self, instance_id: str, *, output: Optional[Any] = None, recursive: bool = True
    ):
        self._portal.call(  # type: ignore
            partial(
                self._async_client.terminate_orchestration,  # type: ignore
                instance_id,
                output=output,
                recursive=recursive,
            )
        )

    def suspend_orchestration(self, instance_id: str):
        self._portal.call(self._async_client.suspend_orchestration, instance_id)  # type: ignore

    def resume_orchestration(self, instance_id: str):
        self._portal.call(self._async_client.resume_orchestration, instance_id)  # type: ignore

    def purge_orchestration(self, instance_id: str, recursive: bool = True):
        self._portal.call(  # type: ignore
            partial(
                self._async_client.purge_orchestration,  # type: ignore
                instance_id,
                recursive=recursive,
            )
        )
