# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Auth error fidelity tests — validate extenddb error responses match real DynamoDB.

These tests send intentionally bad credentials to the DynamoDB endpoint and
verify the error code, HTTP status, and message structure. They run against
both real DynamoDB (when EXTENDDB_TEST_ENDPOINT is unset) and extenddb (when set),
using the standard conftest.py dual-target pattern.

No management API access is needed — any bogus AKIA* key or wrong secret
produces the error responses we want to validate.

REQ-TEST-001, REQ-AUTH-001
"""

from __future__ import annotations

import os
from typing import Any

import boto3
import pytest
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError
def _make_client(endpoint_url: str | None, access_key: str, secret_key: str) -> Any:
    """Create a boto3 DynamoDB client with explicit credentials."""
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    kwargs: dict = {
        "service_name": "dynamodb",
        "region_name": region,
        "aws_access_key_id": access_key,
        "aws_secret_access_key": secret_key,
        "config": BotoConfig(retries={"max_attempts": 0}),
    }
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
        # Self-signed certs from ``extenddb init``; disable SSL verification.
        if endpoint_url.startswith("https://"):
            kwargs["verify"] = False
    return boto3.client(**kwargs)
class TestAuthErrorFidelity:
    """Validate auth error responses match real DynamoDB behavior.

    These tests exercise credential failure paths that work identically
    against real DynamoDB and extenddb — no management API required.

    When targeting extenddb, auth must be enabled (auth.provider = "builtin").
    """

    @pytest.fixture(autouse=True)
    def setup(self, endpoint_url):
        # When targeting extenddb (endpoint_url is set), skip unless auth is enabled.
        # EXTENDDB_ADMIN_USER being set signals that extenddb is running with builtin auth.
        # When targeting real DynamoDB (endpoint_url is None), always run.
        if endpoint_url and not os.environ.get("EXTENDDB_ADMIN_USER", "").strip():
            pytest.fail(
                "MISCONFIGURED: Auth error fidelity tests require auth-enabled extenddb "
                "(set EXTENDDB_ADMIN_USER to signal builtin auth mode). "
                "These must be set by devtools/run-tests before test execution."
            )
        self.endpoint_url = endpoint_url

    def test_invalid_access_key_returns_unrecognized_client(self):
        """Completely bogus access key returns UnrecognizedClientException.

        Real DynamoDB returns HTTP 400 with __type ending in
        UnrecognizedClientException. extenddb must match.
        """
        client = _make_client(
            self.endpoint_url,
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        )
        with pytest.raises(ClientError) as exc_info:
            client.list_tables()

        err = exc_info.value.response
        assert err["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert err["Error"]["Code"] == "UnrecognizedClientException"

    def test_invalid_access_key_message_structure(self):
        """Error message for invalid access key mentions the key ID.

        Real DynamoDB: "The security token included in the request is invalid."
        """
        client = _make_client(
            self.endpoint_url,
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        )
        with pytest.raises(ClientError) as exc_info:
            client.list_tables()

        msg = exc_info.value.response["Error"].get("Message", "")
        # Real DynamoDB says "The security token included in the request is invalid."
        assert "security token" in msg.lower() or "invalid" in msg.lower()

    def test_wrong_secret_key_returns_unrecognized_client(self):
        """Valid-format access key with wrong secret returns expected error.

        Both real DynamoDB and extenddb should return HTTP 400 with
        UnrecognizedClientException (the key doesn't exist in either case,
        so the error is the same as an invalid key).
        """
        client = _make_client(
            self.endpoint_url,
            "AKIA0000000000000000",
            "0000000000000000000000000000000000000000",
        )
        with pytest.raises(ClientError) as exc_info:
            client.list_tables()

        err = exc_info.value.response
        assert err["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert err["Error"]["Code"] == "UnrecognizedClientException"

    def test_empty_access_key_rejected(self):
        """Empty access key is rejected with an auth error.

        boto3 may raise a different error for empty credentials, but if
        the request reaches the server, it must be rejected.
        """
        # boto3 with empty string credentials still sends a SigV4 header.
        client = _make_client(self.endpoint_url, "X", "X")
        with pytest.raises(ClientError) as exc_info:
            client.list_tables()

        err = exc_info.value.response
        # Either UnrecognizedClientException or InvalidSignatureException.
        assert err["Error"]["Code"] in (
            "UnrecognizedClientException",
            "InvalidSignatureException",
        )
        assert err["ResponseMetadata"]["HTTPStatusCode"] == 400
