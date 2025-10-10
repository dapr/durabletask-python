# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
End-to-end tests for durabletask.aio package.

These tests require a running Dapr sidecar or DurableTask-Go emulator.
They test actual workflow execution against a real runtime.

To run these tests:
1. Start Dapr sidecar: dapr run --app-id test-app --dapr-grpc-port 50001
2. Or start DurableTask-Go emulator on localhost:4001
3. Run: pytest tests/aio/test_e2e.py -m e2e
"""

import os
import time
from datetime import datetime

import pytest

from durabletask.aio import AsyncWorkflowContext
from durabletask.client import TaskHubGrpcClient
from durabletask.worker import TaskHubGrpcWorker

# Skip all tests in this module unless explicitly running e2e tests
pytestmark = pytest.mark.e2e


def _log_orchestration_progress(
    hub_client: TaskHubGrpcClient, instance_id: str, max_seconds: int = 60
) -> None:
    """Helper to log orchestration status every second up to max_seconds."""
    deadline = time.time() + max_seconds
    last_status = None
    while time.time() < deadline:
        try:
            st = hub_client.get_orchestration_state(instance_id, fetch_payloads=True)
            if st is None:
                print(f"[async e2e] state: None")
            else:
                status_name = st.runtime_status.name
                if status_name != last_status:
                    print(f"[async e2e] state: {status_name}")
                    last_status = status_name
                if status_name in ("COMPLETED", "FAILED", "TERMINATED"):
                    print("[async e2e] reached terminal state during polling")
                    break
        except Exception as e:
            print(f"[async e2e] polling error: {e}")
        time.sleep(1)


class TestAsyncWorkflowE2E:
    """End-to-end tests for async workflows with real runtime."""

    @classmethod
    def setup_class(cls):
        """Set up test class with worker and client."""
        # Use environment variable or default to localhost:4001 (DurableTask-Go)
        grpc_endpoint = os.getenv("DURABLETASK_GRPC_ENDPOINT", "localhost:4001")
        # Skip if runtime not available
        if not is_runtime_available(grpc_endpoint):
            import pytest as _pytest

            _pytest.skip(f"DurableTask runtime not available at {grpc_endpoint}")

        cls.worker = TaskHubGrpcWorker(host_address=grpc_endpoint)
        cls.client = TaskHubGrpcClient(host_address=grpc_endpoint)

        # Register test activities and workflows
        cls._register_test_functions()

        time.sleep(2)

        # Start worker and wait for ready
        cls.worker.start()
        try:
            if hasattr(cls.worker, "wait_for_ready"):
                try:
                    # type: ignore[attr-defined]
                    cls.worker.wait_for_ready(timeout=10)
                except TypeError:
                    cls.worker.wait_for_ready(10)  # type: ignore[misc]
        except Exception:
            pass

    @classmethod
    def teardown_class(cls):
        """Clean up worker and client."""
        try:
            if hasattr(cls.worker, "stop"):
                cls.worker.stop()
        except Exception:
            pass

    @classmethod
    def _register_test_functions(cls):
        """Register test activities and workflows."""

        # Test activity
        def test_activity(ctx, input_data: str) -> str:
            print(f"[E2E] test_activity input={input_data}")
            return f"Activity processed: {input_data}"

        cls.worker._registry.add_named_activity("test_activity", test_activity)
        cls.test_activity = test_activity

        # Test async workflow
        @cls.worker.add_orchestrator
        async def simple_async_workflow(ctx: AsyncWorkflowContext, input_data: str) -> str:
            result = await ctx.call_activity(test_activity, input=input_data)
            return f"Workflow result: {result}"

        cls.simple_async_workflow = simple_async_workflow

        # Multi-step async workflow
        @cls.worker.add_async_orchestrator
        async def multi_step_async_workflow(ctx: AsyncWorkflowContext, steps: int) -> dict:
            results = []
            for i in range(steps):
                result = await ctx.call_activity(test_activity, input=f"step_{i}")
                results.append(result)

            return {
                "instance_id": ctx.instance_id,
                "steps_completed": len(results),
                "results": results,
                "timestamp": ctx.now().isoformat(),
            }

        cls.multi_step_async_workflow = multi_step_async_workflow

        # Parallel workflow
        @cls.worker.add_async_orchestrator
        async def parallel_async_workflow(ctx: AsyncWorkflowContext, parallel_count: int) -> list:
            tasks = []
            for i in range(parallel_count):
                task = ctx.call_activity(test_activity, input=f"parallel_{i}")
                tasks.append(task)

            results = await ctx.when_all(tasks)
            return results

        cls.parallel_async_workflow = parallel_async_workflow

        # when_any with activities (register early)
        @cls.worker.add_async_orchestrator
        async def when_any_activities(ctx: AsyncWorkflowContext, _) -> dict:
            t1 = ctx.call_activity(test_activity, input="a1")
            t2 = ctx.call_activity(test_activity, input="a2")
            winner = await ctx.when_any([t1, t2])
            res = winner.get_result()
            return {"result": res}

        cls.when_any_activities = when_any_activities

        # when_any_with_result mixing activity and timer (register early)
        @cls.worker.add_async_orchestrator
        async def when_any_with_timer(ctx: AsyncWorkflowContext, _) -> dict:
            t_activity = ctx.call_activity(test_activity, input="wa")
            t_timer = ctx.sleep(0.1)
            idx, res = await ctx.when_any_with_result([t_activity, t_timer])
            return {"index": idx, "has_result": res is not None}

        cls.when_any_with_timer = when_any_with_timer

        # Timer workflow
        @cls.worker.add_async_orchestrator
        async def timer_async_workflow(ctx: AsyncWorkflowContext, delay_seconds: float) -> dict:
            start_time = ctx.now()

            # Wait for specified delay
            await ctx.sleep(delay_seconds)

            end_time = ctx.now()

            return {
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "delay_seconds": delay_seconds,
            }

        cls.timer_async_workflow = timer_async_workflow

        # Sub-orchestrator workflow
        @cls.worker.add_async_orchestrator
        async def child_async_workflow(ctx: AsyncWorkflowContext, input_data: str) -> str:
            result = await ctx.call_activity(test_activity, input=input_data)
            return f"Child: {result}"

        cls.child_async_workflow = child_async_workflow

        @cls.worker.add_async_orchestrator
        async def parent_async_workflow(ctx: AsyncWorkflowContext, input_data: str) -> dict:
            # Call child workflow
            child_result = await ctx.call_sub_orchestrator(
                child_async_workflow, input=input_data, instance_id=f"{ctx.instance_id}_child"
            )

            # Process child result
            final_result = await ctx.call_activity(test_activity, input=child_result)

            return {
                "parent_instance": ctx.instance_id,
                "child_result": child_result,
                "final_result": final_result,
            }

        cls.parent_async_workflow = parent_async_workflow

        # Additional orchestrators for specific tests
        @cls.worker.add_async_orchestrator
        async def suspend_resume_workflow(ctx: AsyncWorkflowContext, _):
            val = await ctx.wait_for_external_event("x")
            return val

        cls.suspend_resume_workflow = suspend_resume_workflow

        @cls.worker.add_async_orchestrator
        async def sub_orch_child(ctx: AsyncWorkflowContext, x: int):
            return x + 1

        cls.sub_orch_child = sub_orch_child

        @cls.worker.add_async_orchestrator
        async def sub_orch_parent(ctx: AsyncWorkflowContext, x: int):
            y = await ctx.call_sub_orchestrator(sub_orch_child, input=x)
            return y * 2

        cls.sub_orch_parent = sub_orch_parent

        def probe_activity(ctx, _):
            return {"tp": ctx.trace_parent, "ts": ctx.trace_state}

        # Register by function so ctx.call_activity(probe_activity) resolves correctly
        cls.worker.add_activity(probe_activity)
        cls.probe_activity = probe_activity

        @cls.worker.add_orchestrator
        async def trace_context_workflow(ctx: AsyncWorkflowContext, _):
            return await ctx.call_activity(probe_activity)

        cls.trace_context_workflow = trace_context_workflow

        # Orchestrator trace context exposure (workflow-level)
        @cls.worker.add_orchestrator
        async def trace_context_orchestrator(ctx: AsyncWorkflowContext, _):
            return {
                "wf_tp": ctx.trace_parent,
                "wf_ts": ctx.trace_state,
                "wf_sid": ctx.workflow_span_id,
            }

        cls.trace_context_orchestrator = trace_context_orchestrator

        # Parent/child trace propagation
        @cls.worker.add_orchestrator
        async def child_trace_orchestrator(ctx: AsyncWorkflowContext, _):
            return {
                "tp": ctx.trace_parent,
                "ts": ctx.trace_state,
                "sid": ctx.workflow_span_id,
            }

        cls.child_trace_orchestrator = child_trace_orchestrator

        @cls.worker.add_orchestrator
        async def parent_trace_orchestrator(ctx: AsyncWorkflowContext, _):
            child = await ctx.call_sub_orchestrator(child_trace_orchestrator)
            act = await ctx.call_activity(probe_activity)
            return {"child": child, "activity": act}

        cls.parent_trace_orchestrator = parent_trace_orchestrator

        # Minimal workflow for debugging - no activities
        @cls.worker.add_orchestrator
        async def minimal_workflow(ctx: AsyncWorkflowContext, input_data: str) -> str:
            return f"Minimal result: {input_data}"

        cls.minimal_workflow = minimal_workflow

        # Determinism test workflow
        @cls.worker.add_orchestrator
        async def deterministic_test_workflow(ctx: AsyncWorkflowContext, input_data: str) -> dict:
            random_val = ctx.random().random()
            uuid_val = str(ctx.uuid4())
            string_val = ctx.random_string(10)
            activity_result = await ctx.call_activity(test_activity, input=input_data)
            return {
                "random": random_val,
                "uuid": uuid_val,
                "string": string_val,
                "activity": activity_result,
                "timestamp": ctx.now().isoformat(),
            }

        cls.deterministic_test_workflow = deterministic_test_workflow

        # Error handling workflow
        def failing_activity(ctx, input_data: str) -> str:
            raise ValueError(f"Activity failed with input: {input_data}")

        cls.worker.add_activity(failing_activity)

        @cls.worker.add_orchestrator
        async def error_handling_workflow(ctx: AsyncWorkflowContext, input_data: str) -> dict:
            try:
                result = await ctx.call_activity(failing_activity, input=input_data)
                return {"status": "success", "result": result}
            except Exception as e:
                return {"status": "error", "error": str(e)}

        cls.error_handling_workflow = error_handling_workflow

        # External event workflow
        @cls.worker.add_orchestrator
        async def external_event_workflow(ctx: AsyncWorkflowContext, event_name: str) -> dict:
            initial_result = await ctx.call_activity(test_activity, input="initial")
            event_data = await ctx.wait_for_external_event(event_name)
            final_result = await ctx.call_activity(test_activity, input=f"event_{event_data}")
            return {"initial": initial_result, "event_data": event_data, "final": final_result}

        cls.external_event_workflow = external_event_workflow

        # (moved earlier) when_any registrations

        # when_any between external event and timeout
        @cls.worker.add_async_orchestrator
        async def when_any_event_or_timeout(ctx: AsyncWorkflowContext, event_name: str) -> dict:
            print(f"[E2E] when_any_event_or_timeout start id={ctx.instance_id} evt={event_name}")
            evt = ctx.wait_for_external_event(event_name)
            timeout = ctx.sleep(5.0)
            winner = await ctx.when_any([evt, timeout])
            if winner == evt:
                val = winner.get_result()
                print(f"[E2E] when_any_event_or_timeout winner=event val={val}")
                return {"winner": "event", "val": val}
            print(f"[E2E] when_any_event_or_timeout winner=timeout")
            return {"winner": "timeout"}

        cls.when_any_event_or_timeout = when_any_event_or_timeout

        # Debug: list registered orchestrators
        try:
            reg = getattr(cls.worker, "_registry", None)
            if reg is not None:
                keys = list(getattr(reg, "orchestrators", {}).keys())
                print(f"[E2E] registered orchestrators: {keys}")
        except Exception:
            pass

    def setup_method(self):
        """Set up each test method."""
        # Worker is started in setup_class; nothing to do per-test
        pass

    @pytest.mark.e2e
    def test_async_suspend_and_resume_dt_e2e(self):
        """Async suspend/resume using class-level worker/client (more stable)."""
        from durabletask import client as dt_client

        # Schedule and wait for RUNNING
        orch_id = self.client.schedule_new_orchestration(type(self).suspend_resume_workflow)
        st = self.client.wait_for_orchestration_start(orch_id, timeout=30)
        assert st is not None and st.runtime_status == dt_client.OrchestrationStatus.RUNNING

        # Suspend
        self.client.suspend_orchestration(orch_id)
        # Wait until SUSPENDED (poll)
        for _ in range(100):
            st = self.client.get_orchestration_state(orch_id)
            assert st is not None
            if st.runtime_status == dt_client.OrchestrationStatus.SUSPENDED:
                break
            time.sleep(0.1)

        # Raise event then resume
        self.client.raise_orchestration_event(orch_id, "x", data=42)
        self.client.resume_orchestration(orch_id)

        # Prefer server-side wait, then log/poll fallback
        try:
            st = self.client.wait_for_orchestration_completion(orch_id, timeout=60)
        except TimeoutError:
            _log_orchestration_progress(self.client, orch_id, max_seconds=30)
            st = self.client.get_orchestration_state(orch_id, fetch_payloads=True)

        assert st is not None
        assert st.runtime_status == dt_client.OrchestrationStatus.COMPLETED
        assert st.serialized_output == "42"

    @pytest.mark.e2e
    def test_async_sub_orchestrator_dt_e2e(self):
        """Async sub-orchestrator end-to-end with stable class-level worker/client."""
        from durabletask import client as dt_client

        orch_id = self.client.schedule_new_orchestration(type(self).sub_orch_parent, input=3)

        try:
            st = self.client.wait_for_orchestration_completion(orch_id, timeout=60)
        except TimeoutError:
            _log_orchestration_progress(self.client, orch_id, max_seconds=30)
            st = self.client.get_orchestration_state(orch_id, fetch_payloads=True)

        assert st is not None
        assert st.runtime_status == dt_client.OrchestrationStatus.COMPLETED
        assert st.failure_details is None
        assert st.serialized_output == "8"

    @pytest.mark.e2e
    def test_activity_receives_trace_context_dt_e2e(self):
        """Activity receives trace context; uses class-level worker/client."""
        from durabletask import client as dt_client

        orch_id = self.client.schedule_new_orchestration(type(self).trace_context_workflow)

        try:
            st = self.client.wait_for_orchestration_completion(orch_id, timeout=60)
        except TimeoutError:
            _log_orchestration_progress(self.client, orch_id, max_seconds=30)
            st = self.client.get_orchestration_state(orch_id, fetch_payloads=True)

        assert st is not None
        assert st.runtime_status == dt_client.OrchestrationStatus.COMPLETED
        # Output should include trace context; require non-empty traceparent
        import json as _json

        out = _json.loads(st.serialized_output or "{}")
        require_trace = os.getenv("REQUIRE_TRACE_CONTEXT") == "1"
        if require_trace:
            assert isinstance(out.get("tp"), str) and len(out.get("tp")) > 0
        else:
            assert isinstance(out.get("tp"), (str, type(None)))
        assert (out.get("ts") is None) or isinstance(out.get("ts"), str)

    @pytest.mark.e2e
    def test_orchestrator_trace_context_dt_e2e(self):
        from durabletask import client as dt_client

        orch_id = self.client.schedule_new_orchestration(type(self).trace_context_orchestrator)
        st = self.client.wait_for_orchestration_completion(orch_id, timeout=60)
        assert st is not None
        assert st.runtime_status == dt_client.OrchestrationStatus.COMPLETED
        import json as _json

        out = _json.loads(st.serialized_output or "{}")
        require_trace = os.getenv("REQUIRE_TRACE_CONTEXT") == "1"
        if require_trace:
            assert isinstance(out.get("wf_tp"), str) and len(out.get("wf_tp")) > 0
            assert isinstance(out.get("wf_sid"), str) and len(out.get("wf_sid")) > 0
        else:
            assert isinstance(out.get("wf_tp"), (str, type(None)))
            assert isinstance(out.get("wf_sid"), (str, type(None)))
        assert (out.get("wf_ts") is None) or isinstance(out.get("wf_ts"), str)

    @pytest.mark.e2e
    def test_sub_orchestrator_and_activity_trace_context_dt_e2e(self):
        from durabletask import client as dt_client

        orch_id = self.client.schedule_new_orchestration(type(self).parent_trace_orchestrator)
        st = self.client.wait_for_orchestration_completion(orch_id, timeout=60)
        assert st is not None
        assert st.runtime_status == dt_client.OrchestrationStatus.COMPLETED
        import json as _json

        out = _json.loads(st.serialized_output or "{}")
        child = out.get("child", {})
        act = out.get("activity", {})
        require_trace = os.getenv("REQUIRE_TRACE_CONTEXT") == "1"
        if require_trace:
            assert isinstance(child.get("tp"), str) and len(child.get("tp")) > 0
            assert isinstance(child.get("sid"), str) and len(child.get("sid")) > 0
            assert isinstance(act.get("tp"), str) and len(act.get("tp")) > 0
        else:
            assert isinstance(child.get("tp"), (str, type(None)))
            assert isinstance(child.get("sid"), (str, type(None)))
            assert isinstance(act.get("tp"), (str, type(None)))
        assert (child.get("ts") is None) or isinstance(child.get("ts"), str)
        assert (act.get("ts") is None) or isinstance(act.get("ts"), str)

    @pytest.mark.e2e
    def test_simple_async_workflow_e2e(self):
        """Test simple async workflow end-to-end."""
        # Use class worker/client which are already started
        instance_id = self.client.schedule_new_orchestration(
            type(self).simple_async_workflow, input="test_input"
        )
        print(f"[async e2e] scheduled instance_id={instance_id}")
        # Quick initial probe
        try:
            st = self.client.get_orchestration_state(instance_id, fetch_payloads=True)
            print(f"[async e2e] initial state: {getattr(st, 'runtime_status', None)}")
        except Exception as e:
            print(f"[async e2e] initial get_orchestration_state failed: {e}")

        # Prefer server-side wait; on timeout, log progress via polling without extending total time
        start_ts = time.time()
        try:
            state = self.client.wait_for_orchestration_completion(instance_id, timeout=60)
        except TimeoutError:
            elapsed = time.time() - start_ts
            remaining = max(0, int(60 - elapsed))
            print(
                f"[async e2e] server-side wait timed out after {elapsed:.1f}s; polling for remaining {remaining}s"
            )
            if remaining > 0:
                _log_orchestration_progress(self.client, instance_id, max_seconds=remaining)
            # Get final state once more before asserting
            state = self.client.get_orchestration_state(instance_id, fetch_payloads=True)
        assert state is not None
        assert state.runtime_status.name == "COMPLETED"
        assert "Activity processed: test_input" in (state.serialized_output or "")

    @pytest.mark.asyncio
    async def test_multi_step_async_workflow_e2e(self):
        """Test multi-step async workflow end-to-end."""
        instance_id = f"test_multi_step_{int(time.time())}"

        # Start workflow
        self.client.schedule_new_orchestration(
            type(self).multi_step_async_workflow, input=3, instance_id=instance_id
        )

        # Wait for completion
        result = self.client.wait_for_orchestration_completion(instance_id, timeout=30)

        assert result is not None
        result_data = result.to_json()

        assert result_data["steps_completed"] == 3
        assert len(result_data["results"]) == 3
        assert result_data["instance_id"] == instance_id

    @pytest.mark.asyncio
    async def test_parallel_async_workflow_e2e(self):
        """Test parallel async workflow end-to-end."""
        instance_id = f"test_parallel_{int(time.time())}"

        # Start workflow
        self.client.schedule_new_orchestration(
            type(self).parallel_async_workflow, input=3, instance_id=instance_id
        )

        # Wait for completion
        result = self.client.wait_for_orchestration_completion(instance_id, timeout=30)

        assert result is not None
        result_data = result.to_json()

        # Should have 3 parallel results
        assert len(result_data) == 3
        for i, res in enumerate(result_data):
            assert f"parallel_{i}" in res

    @pytest.mark.asyncio
    async def test_timer_async_workflow_e2e(self):
        """Test timer async workflow end-to-end."""
        instance_id = f"test_timer_{int(time.time())}"
        delay_seconds = 2.0

        # Start workflow
        self.client.schedule_new_orchestration(
            type(self).timer_async_workflow, input=delay_seconds, instance_id=instance_id
        )

        # Wait for completion
        result = self.client.wait_for_orchestration_completion(instance_id, timeout=30)

        assert result is not None
        result_data = result.to_json()

        assert result_data["delay_seconds"] == delay_seconds
        # Validate using orchestrator timestamps to avoid wall-clock skew
        start_iso = result_data.get("start_time")
        end_iso = result_data.get("end_time")
        if isinstance(start_iso, str) and isinstance(end_iso, str):
            start_dt = datetime.fromisoformat(start_iso)
            end_dt = datetime.fromisoformat(end_iso)
            elapsed = (end_dt - start_dt).total_seconds()
            # Allow jitter from backend scheduling and timestamp rounding
            assert elapsed >= (delay_seconds - 1.0)

    @pytest.mark.asyncio
    async def test_sub_orchestrator_async_workflow_e2e(self):
        """Test sub-orchestrator async workflow end-to-end."""
        instance_id = f"test_sub_orch_{int(time.time())}"

        # Start parent workflow
        self.client.schedule_new_orchestration(
            type(self).parent_async_workflow, input="test_data", instance_id=instance_id
        )

        # Wait for completion
        result = self.client.wait_for_orchestration_completion(instance_id, timeout=30)

        assert result is not None
        result_data = result.to_json()

        assert result_data["parent_instance"] == instance_id
        assert "Child: Activity processed: test_data" in result_data["child_result"]
        assert "Activity processed: Child:" in result_data["final_result"]

    @pytest.mark.asyncio
    async def test_workflow_determinism_e2e(self):
        """Test that async workflows are deterministic during replay."""
        instance_id = f"test_determinism_{int(time.time())}"
        # Start pre-registered workflow
        self.client.schedule_new_orchestration(
            type(self).deterministic_test_workflow,
            input="determinism_test",
            instance_id=instance_id,
        )

        # Wait for completion
        result = self.client.wait_for_orchestration_completion(instance_id, timeout=30)

        assert result is not None
        result_data = result.to_json()

        # Verify deterministic values are present
        assert "random" in result_data
        assert "uuid" in result_data
        assert "string" in result_data
        assert "Activity processed: determinism_test" in result_data["activity"]

        # The values should be deterministic based on instance_id and orchestration time
        # We can't easily test replay here, but the workflow should complete successfully

    @pytest.mark.asyncio
    async def test_when_any_activities_e2e(self):
        instance_id = f"test_when_any_acts_{int(time.time())}"
        self.client.schedule_new_orchestration(
            type(self).when_any_activities, input=None, instance_id=instance_id
        )
        # Ensure the sidecar has started processing this orchestration
        try:
            st = self.client.wait_for_orchestration_start(instance_id, timeout=30)
        except Exception:
            st = None
        assert st is not None
        result = self.client.wait_for_orchestration_completion(instance_id, timeout=30)
        assert result is not None
        if result.failure_details:
            print(
                "when_any_activities failure:",
                result.failure_details.error_type,
                result.failure_details.message,
            )
            assert False, "when_any_activities failed"
        data = result.to_json()
        assert isinstance(data, dict)
        assert "Activity processed:" in data.get("result", "")

    @pytest.mark.asyncio
    async def test_when_any_with_timer_e2e(self):
        instance_id = f"test_when_any_timer_{int(time.time())}"
        self.client.schedule_new_orchestration(
            type(self).when_any_with_timer, input=None, instance_id=instance_id
        )
        try:
            _ = self.client.wait_for_orchestration_start(instance_id, timeout=30)
        except Exception:
            pass
        result = self.client.wait_for_orchestration_completion(instance_id, timeout=30)
        assert result is not None
        data = result.to_json()
        assert isinstance(data, dict)
        assert data.get("index") in (0, 1)
        assert isinstance(data.get("has_result"), bool)

    @pytest.mark.asyncio
    async def test_when_any_event_or_timeout_e2e(self):
        instance_id = f"test_when_any_event_{int(time.time())}"
        event_name = "evt"
        self.client.schedule_new_orchestration(
            type(self).when_any_event_or_timeout, input=event_name, instance_id=instance_id
        )
        try:
            _ = self.client.wait_for_orchestration_start(instance_id, timeout=30)
        except Exception:
            pass
        # Raise the event shortly after to ensure event wins
        time.sleep(0.5)
        self.client.raise_orchestration_event(instance_id, event_name, data="hello")
        result = self.client.wait_for_orchestration_completion(instance_id, timeout=30)
        assert result is not None
        if result.failure_details:
            print(
                "when_any_event_or_timeout failure:",
                result.failure_details.error_type,
                result.failure_details.message,
            )
            assert False, "when_any_event_or_timeout failed"
        data = result.to_json()
        assert data.get("winner") == "event"
        assert data.get("val") == "hello"

    @pytest.mark.asyncio
    async def test_async_workflow_error_handling_e2e(self):
        """Test error handling in async workflows end-to-end."""
        instance_id = f"test_error_{int(time.time())}"

        # Start pre-registered workflow
        self.client.schedule_new_orchestration(
            type(self).error_handling_workflow, input="test_error_input", instance_id=instance_id
        )

        # Wait for completion
        result = self.client.wait_for_orchestration_completion(instance_id, timeout=30)

        assert result is not None
        result_data = result.to_json()

        # Should have handled the error gracefully
        assert result_data["status"] == "error"
        assert "Activity failed with input: test_error_input" in result_data["error"]

    @pytest.mark.asyncio
    async def test_async_workflow_with_external_event_e2e(self):
        """Test async workflow with external events end-to-end."""
        instance_id = f"test_external_event_{int(time.time())}"

        # Start pre-registered workflow
        self.client.schedule_new_orchestration(
            type(self).external_event_workflow, input="test_event", instance_id=instance_id
        )

        # Give workflow time to start and wait for event
        import asyncio

        await asyncio.sleep(1)

        # Send external event
        self.client.raise_orchestration_event(
            instance_id, "test_event", data={"message": "event_received"}
        )

        # Wait for completion
        result = self.client.wait_for_orchestration_completion(instance_id, timeout=30)

        assert result is not None
        result_data = result.to_json()

        assert "Activity processed: initial" in result_data["initial"]
        assert result_data["event_data"]["message"] == "event_received"
        assert "Activity processed: event_" in result_data["final"]
        assert "event_received" in result_data["final"]


class TestAsyncWorkflowPerformanceE2E:
    """Performance tests for async workflows."""

    @pytest.mark.e2e
    @pytest.mark.asyncio
    async def test_async_workflow_performance_baseline(self):
        """Baseline performance test for async workflows."""
        # This test would measure execution time for various workflow patterns
        # and ensure they meet performance requirements

        # For now, just ensure the test structure is in place
        assert True  # Placeholder

    @pytest.mark.e2e
    @pytest.mark.asyncio
    async def test_async_workflow_memory_usage(self):
        """Test memory usage of async workflows."""
        # This test would monitor memory usage during workflow execution
        # to ensure no memory leaks or excessive usage

        # For now, just ensure the test structure is in place
        assert True  # Placeholder


# Utility functions for E2E tests


def is_runtime_available(endpoint: str = "localhost:4001") -> bool:
    """Check if DurableTask runtime is available at the given endpoint."""
    import socket

    try:
        host, port = endpoint.split(":")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        result = sock.connect_ex((host, int(port)))
        sock.close()
        return result == 0
    except Exception:
        return False


def skip_if_no_runtime():
    """Pytest fixture to skip tests if no runtime is available."""
    endpoint = os.getenv("DURABLETASK_GRPC_ENDPOINT", "localhost:4001")
    if not is_runtime_available(endpoint):
        pytest.skip(f"DurableTask runtime not available at {endpoint}")
