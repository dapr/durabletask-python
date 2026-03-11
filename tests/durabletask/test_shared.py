"""
Copyright 2025 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from durabletask.internal.shared import (
    DEFAULT_GRPC_KEEPALIVE_OPTIONS,
    _merge_grpc_options,
)


class TestMergeGrpcOptions:
    def test_user_options_take_precedence(self):
        """User-supplied options override defaults with the same key."""
        user_options = [
            ("grpc.keepalive_time_ms", 60_000),
            ("grpc.custom_option", 42),
        ]
        result = _merge_grpc_options(user_options)
        result_dict = dict(result)

        # User override should win
        assert result_dict["grpc.keepalive_time_ms"] == 60_000
        # User-only option should be present
        assert result_dict["grpc.custom_option"] == 42
        # Non-overridden defaults should still be present
        assert result_dict["grpc.keepalive_timeout_ms"] == 10_000
        assert result_dict["grpc.http2.max_pings_without_data"] == 0
        assert result_dict["grpc.keepalive_permit_without_calls"] == 1

    def test_defaults_used_when_no_user_options(self):
        """When user passes an empty sequence, all defaults are returned."""
        result = _merge_grpc_options([])
        assert result == list(DEFAULT_GRPC_KEEPALIVE_OPTIONS)

    def test_none_user_options(self):
        """When user passes None, all defaults are returned."""
        result = _merge_grpc_options(None)
        assert result == list(DEFAULT_GRPC_KEEPALIVE_OPTIONS)
