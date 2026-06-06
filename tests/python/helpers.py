# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Shared helpers for the comprehensive test suite.

These are plain functions (not fixtures) that test modules import directly.
Fixtures live in conftest.py and are auto-discovered by pytest.
"""

from __future__ import annotations

import time
import uuid


def wait_for_active(client, table_name: str, timeout: float = 60.0) -> None:
    """Poll DescribeTable until status is ACTIVE with exponential backoff."""
    deadline = time.monotonic() + timeout
    delay = 0.1
    while time.monotonic() < deadline:
        resp = client.describe_table(TableName=table_name)
        if resp["Table"]["TableStatus"] == "ACTIVE":
            return
        time.sleep(max(0, min(delay, deadline - time.monotonic())))
        delay = min(delay * 2, 2.0)
    raise TimeoutError(f"Table {table_name} did not become ACTIVE within {timeout}s")


def wait_for_deleted(client, table_name: str, timeout: float = 60.0) -> None:
    """Poll DescribeTable until the table no longer exists."""
    deadline = time.monotonic() + timeout
    delay = 0.1
    while time.monotonic() < deadline:
        try:
            client.describe_table(TableName=table_name)
        except client.exceptions.ResourceNotFoundException:
            return
        time.sleep(max(0, min(delay, deadline - time.monotonic())))
        delay = min(delay * 2, 2.0)
    raise TimeoutError(f"Table {table_name} was not deleted within {timeout}s")


def unique_name(prefix: str = "test") -> str:
    """Generate a unique name for test isolation."""
    return f"{prefix}-{uuid.uuid4().hex[:12]}"
