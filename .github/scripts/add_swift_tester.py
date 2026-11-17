#!/usr/bin/env python3
#
# add_swift_tester.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Register the Swift stack tester in an older FoundationDB `known_testers.py`.

FoundationDB only lists the Swift tester from 8.0.0 on. To run the bindingtester from an
earlier release (for instance to compare the Swift bindings against an older client), this adds
the same entry that 8.0.0 uses. It does nothing if the file already knows about Swift.

Usage: add_swift_tester.py <path to bindings/bindingtester/known_testers.py>
"""

import re
import sys
from pathlib import Path

# The same entry as FoundationDB 8.0.0. It uses only keyword arguments that every supported
# release's `Tester` accepts, and tenants stay disabled, which is the default.
SWIFT_ENTRY = '''    "swift": Tester(
        "swift",
        _absolute_path("swift/bin/stacktester_swift"),
        63,
        730,
        MAX_API_VERSION,
        directory_snapshot_ops_enabled=False,
    ),
'''


def main(argv) -> int:
    if len(argv) != 1:
        print(__doc__, file=sys.stderr)
        return 2
    path = Path(argv[0])
    text = path.read_text()

    if re.search(r"""^\s*["']swift["']\s*:""", text, re.MULTILINE):
        print(f"{path}: already has a swift tester, nothing to do")
        return 0

    # Insert before the closing brace of the `testers = { ... }` dictionary.
    start = text.find("\ntesters = {")
    end = text.find("\n}", start) if start != -1 else -1
    if end == -1:
        print(f"error: could not find the `testers = {{ ... }}` dictionary in {path}", file=sys.stderr)
        return 1
    path.write_text(text[: end + 1] + SWIFT_ENTRY + text[end + 1 :])
    print(f"{path}: added the swift tester")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
