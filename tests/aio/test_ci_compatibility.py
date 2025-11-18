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
CI/CD compatibility tests for AsyncWorkflowContext.

These tests are designed to be run in continuous integration to catch
compatibility regressions early and ensure smooth upstream merges.
"""

import pytest

from durabletask.aio import AsyncWorkflowContext

from .compatibility_utils import (
    CompatibilityChecker,
    check_async_context_compatibility,
    validate_runtime_compatibility,
)


class TestCICompatibility:
    """CI/CD compatibility validation tests."""

    def test_async_context_maintains_full_compatibility(self):
        """
        Critical test: Ensure AsyncWorkflowContext maintains full compatibility.

        This test should NEVER fail in CI. If it does, it indicates a breaking
        change that could cause issues with upstream merges or existing code.
        """
        report = check_async_context_compatibility()

        assert report["is_compatible"], (
            f"CRITICAL: AsyncWorkflowContext compatibility broken! "
            f"Missing members: {report['missing_members']}"
        )

        # Ensure we have no missing members
        assert len(report["missing_members"]) == 0, (
            f"Missing required members: {report['missing_members']}"
        )

    def test_no_regression_in_base_interface(self):
        """
        Test that no base OrchestrationContext interface members are missing.

        This catches regressions where required properties or methods are
        accidentally removed or renamed.
        """
        from unittest.mock import Mock

        import durabletask.task as dt_task

        # Create a test instance
        mock_base_ctx = Mock(spec=dt_task.OrchestrationContext)
        mock_base_ctx.instance_id = "ci-test"
        mock_base_ctx.current_utc_datetime = None
        mock_base_ctx.is_replaying = False

        async_ctx = AsyncWorkflowContext(mock_base_ctx)

        # Validate runtime compatibility
        missing_items = CompatibilityChecker.validate_context_compatibility(async_ctx)

        assert len(missing_items) == 0, (
            f"Compatibility regression detected! Missing: {missing_items}"
        )

    def test_runtime_validation_passes(self):
        """
        Test that runtime validation passes for AsyncWorkflowContext.

        This ensures the context can be used wherever OrchestrationContext
        is expected without runtime errors.
        """
        from unittest.mock import Mock

        import durabletask.task as dt_task

        mock_base_ctx = Mock(spec=dt_task.OrchestrationContext)
        mock_base_ctx.instance_id = "runtime-test"
        mock_base_ctx.current_utc_datetime = None

        async_ctx = AsyncWorkflowContext(mock_base_ctx)

        # This should pass without warnings or errors
        is_valid = validate_runtime_compatibility(async_ctx, strict=True)
        assert is_valid, "Runtime validation failed"

    def test_enhanced_methods_are_additive_only(self):
        """
        Test that enhanced methods are purely additive and don't break base functionality.

        This ensures that new async-specific methods don't interfere with
        the base OrchestrationContext interface.
        """
        report = check_async_context_compatibility()

        # We should have extra methods (enhancements) but no missing ones
        assert len(report["extra_members"]) > 0, "No enhanced methods found"
        assert len(report["missing_members"]) == 0, "Base methods are missing"

        # Verify some expected enhancements exist
        extra_methods = [
            item.split(": ")[1] for item in report["extra_members"] if "method:" in item
        ]
        expected_enhancements = ["sleep", "activity", "when_all", "when_any", "gather"]

        for enhancement in expected_enhancements:
            assert enhancement in extra_methods, f"Expected enhancement '{enhancement}' not found"

    def test_protocol_compliance_at_class_level(self):
        """
        Test that AsyncWorkflowContext class complies with the protocol.

        This is a compile-time style check that validates the class structure
        without needing to instantiate it.
        """
        is_compliant = CompatibilityChecker.check_protocol_compliance(AsyncWorkflowContext)
        assert is_compliant, (
            "AsyncWorkflowContext does not comply with OrchestrationContextProtocol"
        )

    @pytest.mark.parametrize(
        "property_name",
        [
            "instance_id",
            "current_utc_datetime",
            "is_replaying",
            "workflow_name",
            "parent_instance_id",
            "history_event_sequence",
            "is_suspended",
        ],
    )
    def test_required_property_exists(self, property_name):
        """
        Test that each required property exists on AsyncWorkflowContext.

        This parameterized test ensures all OrchestrationContext properties
        are available on AsyncWorkflowContext.
        """
        assert hasattr(AsyncWorkflowContext, property_name), (
            f"Required property '{property_name}' missing from AsyncWorkflowContext"
        )

    @pytest.mark.parametrize(
        "method_name",
        [
            "set_custom_status",
            "create_timer",
            "call_activity",
            "call_sub_orchestrator",
            "wait_for_external_event",
            "continue_as_new",
        ],
    )
    def test_required_method_exists(self, method_name):
        """
        Test that each required method exists on AsyncWorkflowContext.

        This parameterized test ensures all OrchestrationContext methods
        are available on AsyncWorkflowContext.
        """
        assert hasattr(AsyncWorkflowContext, method_name), (
            f"Required method '{method_name}' missing from AsyncWorkflowContext"
        )

        method = getattr(AsyncWorkflowContext, method_name)
        assert callable(method), f"Required method '{method_name}' is not callable"


class TestUpstreamMergeReadiness:
    """Tests to ensure readiness for upstream merges."""

    def test_no_breaking_changes_in_public_api(self):
        """
        Test that the public API hasn't changed in breaking ways.

        This test helps ensure that upstream merges won't break existing
        code that depends on AsyncWorkflowContext.
        """
        report = check_async_context_compatibility()

        # Should have all base members
        base_member_count = len(report["base_members"])
        context_member_count = len(report["context_members"])

        assert context_member_count >= base_member_count, (
            "AsyncWorkflowContext has fewer members than OrchestrationContext"
        )

    def test_backward_compatibility_maintained(self):
        """
        Test that backward compatibility is maintained.

        This ensures that code written against the base OrchestrationContext
        interface will continue to work with AsyncWorkflowContext.
        """
        from unittest.mock import Mock

        import durabletask.task as dt_task

        mock_base_ctx = Mock(spec=dt_task.OrchestrationContext)
        mock_base_ctx.instance_id = "backward-compat-test"
        mock_base_ctx.current_utc_datetime = None
        mock_base_ctx.is_replaying = False

        async_ctx = AsyncWorkflowContext(mock_base_ctx)

        # Test that it can be used in functions expecting OrchestrationContext
        def function_expecting_base_context(ctx):
            # This should work without any issues
            return {
                "id": ctx.instance_id,
                "replaying": ctx.is_replaying,
                "time": ctx.current_utc_datetime,
            }

        # This should not raise any errors
        result = function_expecting_base_context(async_ctx)
        assert result["id"] == "backward-compat-test"
        assert result["replaying"] is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
