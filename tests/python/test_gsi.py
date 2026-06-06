# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Global Secondary Index (GSI) tests.

Covers creating tables with GSIs, querying GSIs, item projection
into GSIs, and GSI-specific behaviors.
"""

from __future__ import annotations

import time

import pytest
from botocore.exceptions import ClientError

from helpers import unique_name, wait_for_active, wait_for_deleted


def _gsi_table(dynamodb_client, table_name: str | None = None) -> str:
    """Create a table with a GSI on 'gsi_pk' (HASH) and 'gsi_sk' (RANGE)."""
    name = table_name or unique_name("gsi")
    dynamodb_client.create_table(
        TableName=name,
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "gsi_pk", "AttributeType": "S"},
            {"AttributeName": "gsi_sk", "AttributeType": "S"},
        ],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
        GlobalSecondaryIndexes=[
            {
                "IndexName": "gsi-index",
                "KeySchema": [
                    {"AttributeName": "gsi_pk", "KeyType": "HASH"},
                    {"AttributeName": "gsi_sk", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
    )
    wait_for_active(dynamodb_client, name)
    return name


class TestGSICreate:
    """GSI creation and describe tests."""

    def test_create_table_with_gsi(self, dynamodb_client):
        """Create a table with a GSI and verify it appears in DescribeTable."""
        name = _gsi_table(dynamodb_client)
        try:
            resp = dynamodb_client.describe_table(TableName=name)
            table = resp["Table"]
            assert len(table["GlobalSecondaryIndexes"]) == 1
            gsi = table["GlobalSecondaryIndexes"][0]
            assert gsi["IndexName"] == "gsi-index"
            assert gsi["IndexStatus"] == "ACTIVE"
            ks = {k["AttributeName"]: k["KeyType"] for k in gsi["KeySchema"]}
            assert ks == {"gsi_pk": "HASH", "gsi_sk": "RANGE"}
        finally:
            dynamodb_client.delete_table(TableName=name)
            wait_for_deleted(dynamodb_client, name)

    def test_create_table_with_keys_only_projection(self, dynamodb_client):
        """GSI with KEYS_ONLY projection."""
        name = unique_name("gsi")
        dynamodb_client.create_table(
            TableName=name,
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "gsi_pk", "AttributeType": "S"},
            ],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "keys-only-idx",
                    "KeySchema": [{"AttributeName": "gsi_pk", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "KEYS_ONLY"},
                }
            ],
        )
        wait_for_active(dynamodb_client, name)
        try:
            resp = dynamodb_client.describe_table(TableName=name)
            gsi = resp["Table"]["GlobalSecondaryIndexes"][0]
            assert gsi["Projection"]["ProjectionType"] == "KEYS_ONLY"
        finally:
            dynamodb_client.delete_table(TableName=name)
            wait_for_deleted(dynamodb_client, name)

    def test_create_table_with_include_projection(self, dynamodb_client):
        """GSI with INCLUDE projection."""
        name = unique_name("gsi")
        dynamodb_client.create_table(
            TableName=name,
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "gsi_pk", "AttributeType": "S"},
            ],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "include-idx",
                    "KeySchema": [{"AttributeName": "gsi_pk", "KeyType": "HASH"}],
                    "Projection": {
                        "ProjectionType": "INCLUDE",
                        "NonKeyAttributes": ["extra_field"],
                    },
                }
            ],
        )
        wait_for_active(dynamodb_client, name)
        try:
            resp = dynamodb_client.describe_table(TableName=name)
            gsi = resp["Table"]["GlobalSecondaryIndexes"][0]
            assert gsi["Projection"]["ProjectionType"] == "INCLUDE"
            assert "extra_field" in gsi["Projection"]["NonKeyAttributes"]
        finally:
            dynamodb_client.delete_table(TableName=name)
            wait_for_deleted(dynamodb_client, name)


class TestGSIQuery:
    """GSI query tests."""

    @pytest.fixture(autouse=True, scope="class")
    def _setup(self, dynamodb_client, request):
        """Create a GSI table with test data (class-scoped)."""
        name = _gsi_table(dynamodb_client)
        # Insert items — some with GSI keys, some without
        items = [
            {"pk": {"S": "p1"}, "gsi_pk": {"S": "g1"}, "gsi_sk": {"S": "a"}, "v": {"N": "1"}},
            {"pk": {"S": "p2"}, "gsi_pk": {"S": "g1"}, "gsi_sk": {"S": "b"}, "v": {"N": "2"}},
            {"pk": {"S": "p3"}, "gsi_pk": {"S": "g1"}, "gsi_sk": {"S": "c"}, "v": {"N": "3"}},
            {"pk": {"S": "p4"}, "gsi_pk": {"S": "g2"}, "gsi_sk": {"S": "a"}, "v": {"N": "4"}},
            {"pk": {"S": "p5"}},  # No GSI keys — should not appear in GSI
        ]
        for item in items:
            dynamodb_client.put_item(TableName=name, Item=item)
        # Allow GSI propagation
        time.sleep(2)
        request.cls._table_name = name
        request.cls._client = dynamodb_client
        yield
        dynamodb_client.delete_table(TableName=name)
        wait_for_deleted(dynamodb_client, name)

    def test_query_gsi_partition(self):
        """Query GSI by partition key."""
        resp = self._client.query(
            TableName=self._table_name,
            IndexName="gsi-index",
            KeyConditionExpression="gsi_pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "g1"}},
        )
        assert resp["Count"] == 3

    def test_query_gsi_with_sort_key(self):
        """Query GSI with sort key condition."""
        resp = self._client.query(
            TableName=self._table_name,
            IndexName="gsi-index",
            KeyConditionExpression="gsi_pk = :pk AND gsi_sk = :sk",
            ExpressionAttributeValues={":pk": {"S": "g1"}, ":sk": {"S": "b"}},
        )
        assert resp["Count"] == 1
        assert resp["Items"][0]["v"]["N"] == "2"

    def test_query_gsi_begins_with(self):
        """Query GSI with begins_with on sort key."""
        resp = self._client.query(
            TableName=self._table_name,
            IndexName="gsi-index",
            KeyConditionExpression="gsi_pk = :pk AND begins_with(gsi_sk, :prefix)",
            ExpressionAttributeValues={":pk": {"S": "g1"}, ":prefix": {"S": "a"}},
        )
        assert resp["Count"] == 1

    def test_query_gsi_different_partition(self):
        """Query GSI for a different partition."""
        resp = self._client.query(
            TableName=self._table_name,
            IndexName="gsi-index",
            KeyConditionExpression="gsi_pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "g2"}},
        )
        assert resp["Count"] == 1
        assert resp["Items"][0]["v"]["N"] == "4"

    def test_query_gsi_empty_partition(self):
        """Query GSI for a nonexistent partition returns empty."""
        resp = self._client.query(
            TableName=self._table_name,
            IndexName="gsi-index",
            KeyConditionExpression="gsi_pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "nonexistent"}},
        )
        assert resp["Count"] == 0
        assert resp["Items"] == []

    def test_query_gsi_select_count(self):
        """Query GSI with SELECT COUNT."""
        resp = self._client.query(
            TableName=self._table_name,
            IndexName="gsi-index",
            KeyConditionExpression="gsi_pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "g1"}},
            Select="COUNT",
        )
        assert resp["Count"] == 3
        assert "Items" not in resp or resp["Items"] == []

    def test_scan_gsi(self):
        """Scan the GSI returns all indexed items."""
        resp = self._client.scan(
            TableName=self._table_name,
            IndexName="gsi-index",
        )
        # p5 has no GSI keys, so should not appear
        assert resp["Count"] == 4

    def test_query_nonexistent_gsi(self):
        """Query a nonexistent GSI returns ValidationException."""
        with pytest.raises(ClientError) as exc:
            self._client.query(
                TableName=self._table_name,
                IndexName="no-such-index",
                KeyConditionExpression="gsi_pk = :pk",
                ExpressionAttributeValues={":pk": {"S": "g1"}},
            )
        assert exc.value.response["Error"]["Code"] == "ValidationException"
