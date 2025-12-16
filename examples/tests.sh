#!/usr/bin/env bash

#  Copyright 2025 The Dapr Authors
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#      http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

set -Eeuo pipefail
trap 'exit 1' ERR

## Activity Sequence
python activity_sequence.py > activity_output
grep "Hello Tokyo!" activity_output


## Fan Out/Fan In
python fanout_fanin.py > fanout_fanin_output
grep "Orchestration completed! Result" fanout_fanin_output

## Wait for Event
{ sleep 2; printf '\n'; } | python human_interaction.py --timeout 20 --approver PYTHON-CI > human_interaction_ouptput
grep "Orchestration completed! Result: \"Approved by 'PYTHON-CI'\"" human_interaction_ouptput


