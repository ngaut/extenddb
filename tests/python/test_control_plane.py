# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Miscellaneous control plane tests: DescribeEndpoints, DescribeLimits,
DescribeTable details, GSI in DescribeTable.

Covers scenarios from external suite: MiscControlPlaneTests.
Tests run identically against real DynamoDB and extenddb.
"""

from __future__ import annotations

import os
import uuid

import pytest
from botocore.exceptions import ClientError

from helpers import wait_for_deleted

_extenddb_only = pytest.mark.skipif(
    os.environ.get("EXTENDDB_VALIDATION_MODE", "").lower() == "true",
    reason="extenddb_only: skipped in validation mode",
)


class TestMiscControlPlane:
    """Miscellaneous control plane operations."""

    @_extenddb_only
    def test_describe_endpoints(self, dynamodb_client):
        """DescribeEndpoints returns at least one endpoint."""
        resp = dynamodb_client.describe_endpoints()
        assert len(resp["Endpoints"]) >= 1
        ep = resp["Endpoints"][0]
        assert "Address" in ep
        assert "CachePeriodInMinutes" in ep

    def test_describe_limits(self, dynamodb_client):
        """DescribeLimits returns limit values."""
        resp = dynamodb_client.describe_limits()
        assert "AccountMaxReadCapacityUnits" in resp
        assert "AccountMaxWriteCapacityUnits" in resp
        assert "TableMaxReadCapacityUnits" in resp
        assert "TableMaxWriteCapacityUnits" in resp

    def test_describe_table_arn(self, table_factory, dynamodb_client):
        """DescribeTable returns a valid TableArn."""
        name = table_factory()
        resp = dynamodb_client.describe_table(TableName=name)
        arn = resp["Table"]["TableArn"]
        assert arn.startswith("arn:")
        assert name in arn

    def test_describe_table_returns_key_schema(self, table_factory, dynamodb_client):
        """DescribeTable returns correct KeySchema."""
        name = table_factory(range_key="sk")
        resp = dynamodb_client.describe_table(TableName=name)
        ks = {k["AttributeName"]: k["KeyType"] for k in resp["Table"]["KeySchema"]}
        assert ks["pk"] == "HASH"
        assert ks["sk"] == "RANGE"

    def test_describe_table_returns_attribute_definitions(
        self, table_factory, dynamodb_client
    ):
        """DescribeTable returns correct AttributeDefinitions."""
        name = table_factory(range_key="sk")
        resp = dynamodb_client.describe_table(TableName=name)
        attrs = {
            a["AttributeName"]: a["AttributeType"]
            for a in resp["Table"]["AttributeDefinitions"]
        }
        assert attrs["pk"] == "S"
        assert attrs["sk"] == "S"

    def test_describe_table_status(self, table_factory, dynamodb_client):
        """DescribeTable returns ACTIVE status after creation."""
        name = table_factory()
        resp = dynamodb_client.describe_table(TableName=name)
        assert resp["Table"]["TableStatus"] == "ACTIVE"

    def test_describe_table_nonexistent(self, dynamodb_client):
        """DescribeTable on nonexistent table returns ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            dynamodb_client.describe_table(
                TableName=f"nonexistent-{uuid.uuid4().hex[:8]}"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_table_with_gsi(self, table_factory, dynamodb_client):
        """DescribeTable returns GSI information."""
        name = table_factory(
            range_key="sk",
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "gsi_pk", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "gsi-test",
                    "KeySchema": [
                        {"AttributeName": "gsi_pk", "KeyType": "HASH"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
        )
        resp = dynamodb_client.describe_table(TableName=name)
        gsis = resp["Table"].get("GlobalSecondaryIndexes", [])
        assert len(gsis) >= 1
        gsi_names = [g["IndexName"] for g in gsis]
        assert "gsi-test" in gsi_names

    def test_list_tables_returns_created(self, table_factory, dynamodb_client):
        """ListTables includes recently created tables."""
        name = table_factory()
        resp = dynamodb_client.list_tables()
        assert name in resp["TableNames"]

    def test_list_tables_pagination(self, table_factory, dynamodb_client):
        """ListTables pagination with Limit."""
        names = [table_factory() for _ in range(3)]
        resp = dynamodb_client.list_tables(Limit=1)
        assert len(resp["TableNames"]) == 1
        assert "LastEvaluatedTableName" in resp

    def test_list_tables_with_limit(self, table_factory, dynamodb_client):
        """ListTables with Limit returns at most Limit tables."""
        for _ in range(3):
            table_factory()
        resp = dynamodb_client.list_tables(Limit=2)
        assert len(resp["TableNames"]) <= 2


class TestTableOperationsEdgeCases:
    """Table operation edge cases from external suite."""

    def test_create_table_with_all_key_types(self, table_factory, dynamodb_client):
        """Create tables with S, N, B key types."""
        for key_type in ("S", "N", "B"):
            name = table_factory(hash_key="k", hash_type=key_type)
            resp = dynamodb_client.describe_table(TableName=name)
            attrs = {
                a["AttributeName"]: a["AttributeType"]
                for a in resp["Table"]["AttributeDefinitions"]
            }
            assert attrs["k"] == key_type

    def test_create_table_with_provisioned_throughput(
        self, table_factory, dynamodb_client
    ):
        """Create table with provisioned throughput."""
        name = table_factory(
            BillingMode="PROVISIONED",
            ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 5},
        )
        resp = dynamodb_client.describe_table(TableName=name)
        pt = resp["Table"]["ProvisionedThroughput"]
        assert pt["ReadCapacityUnits"] == 10
        assert pt["WriteCapacityUnits"] == 5

    def test_create_table_with_gsi_keys_only(self, table_factory, dynamodb_client):
        """Create table with GSI using KEYS_ONLY projection."""
        name = table_factory(
            range_key="sk",
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "gsi_pk", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "gsi-keys",
                    "KeySchema": [
                        {"AttributeName": "gsi_pk", "KeyType": "HASH"},
                    ],
                    "Projection": {"ProjectionType": "KEYS_ONLY"},
                }
            ],
        )
        resp = dynamodb_client.describe_table(TableName=name)
        gsi = resp["Table"]["GlobalSecondaryIndexes"][0]
        assert gsi["Projection"]["ProjectionType"] == "KEYS_ONLY"

    def test_delete_table_twice(self, table_factory, dynamodb_client):
        """Deleting a table twice returns ResourceNotFoundException on second attempt."""
        name = table_factory()
        dynamodb_client.delete_table(TableName=name)
        wait_for_deleted(dynamodb_client, name)
        with pytest.raises(ClientError) as exc:
            dynamodb_client.delete_table(TableName=name)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
