# Copyright 2025 The Dapr Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Tests for app_id propagation through aio AsyncWorkflowContext and awaitables.
"""

from __future__ import annotations

from typing import Any, Dict, Optional
from unittest.mock import Mock

import durabletask.task as dt_task
from durabletask.aio import AsyncWorkflowContext


def test_activity_app_id_passed_to_base_ctx_when_supported():
    base_ctx = Mock(spec=dt_task.OrchestrationContext)

    # Mock call_activity signature to include app_id and metadata
    def _call_activity(
        activity: Any,
        *,
        input: Any = None,
        retry_policy: Any = None,
        app_id: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
    ):
        # Return a durable task-like object; tests only need that it's called with kwargs
        return dt_task.when_all([])

    base_ctx.call_activity = _call_activity  # type: ignore[attr-defined]

    async_ctx = AsyncWorkflowContext(base_ctx)

    awaitable = async_ctx.call_activity(
        "do_work", input={"x": 1}, retry_policy=None, app_id="target-app", metadata={"k": "v"}
    )
    task_obj = awaitable._to_task()
    assert isinstance(task_obj, dt_task.Task)


def test_sub_orchestrator_app_id_passed_to_base_ctx_when_supported():
    base_ctx = Mock(spec=dt_task.OrchestrationContext)

    # Mock call_sub_orchestrator signature to include app_id and metadata
    def _call_sub(
        orchestrator: Any,
        *,
        input: Any = None,
        instance_id: Optional[str] = None,
        retry_policy: Any = None,
        app_id: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
    ):
        return dt_task.when_all([])

    base_ctx.call_sub_orchestrator = _call_sub  # type: ignore[attr-defined]

    async_ctx = AsyncWorkflowContext(base_ctx)

    awaitable = async_ctx.sub_orchestrator(
        "child_wf",
        input=None,
        instance_id="abc",
        retry_policy=None,
        app_id="target-app",
        metadata={"k2": "v2"},
    )
    task_obj = awaitable._to_task()
    assert isinstance(task_obj, dt_task.Task)


def test_activity_app_id_not_passed_when_not_supported():
    base_ctx = Mock(spec=dt_task.OrchestrationContext)

    # Mock call_activity without app_id support
    def _call_activity(
        activity: Any,
        *,
        input: Any = None,
        retry_policy: Any = None,
        metadata: Optional[Dict[str, str]] = None,
    ):
        return dt_task.when_all([])

    base_ctx.call_activity = _call_activity  # type: ignore[attr-defined]

    async_ctx = AsyncWorkflowContext(base_ctx)

    awaitable = async_ctx.call_activity(
        "do_work", input={"x": 1}, retry_policy=None, app_id="target-app", metadata={"k": "v"}
    )
    task_obj = awaitable._to_task()
    assert isinstance(task_obj, dt_task.Task)


def test_sub_orchestrator_app_id_not_passed_when_not_supported():
    base_ctx = Mock(spec=dt_task.OrchestrationContext)

    # Mock call_sub_orchestrator without app_id support
    def _call_sub(
        orchestrator: Any,
        *,
        input: Any = None,
        instance_id: Optional[str] = None,
        retry_policy: Any = None,
        metadata: Optional[Dict[str, str]] = None,
    ):
        return dt_task.when_all([])

    base_ctx.call_sub_orchestrator = _call_sub  # type: ignore[attr-defined]

    async_ctx = AsyncWorkflowContext(base_ctx)

    awaitable = async_ctx.sub_orchestrator(
        "child_wf",
        input=None,
        instance_id="abc",
        retry_policy=None,
        app_id="target-app",
        metadata={"k2": "v2"},
    )
    task_obj = awaitable._to_task()
    assert isinstance(task_obj, dt_task.Task)
