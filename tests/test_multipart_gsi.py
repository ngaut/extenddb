# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Tests for multi-part GSI keys.

GSIs support up to 4 HASH + 4 RANGE key schema elements.
These tests verify table creation, item writes, and queries
against GSIs with composite (multi-part) keys.
"""

from __future__ import annotations

import time
import uuid

import pytest
from conftest import wait_for_active, wait_for_deleted
def wait_for_gsi_count(
    client, table_name: str, index_name: str,
    key_expr: str, expr_values: dict, expected: int,
    timeout: float = 5.0,
) -> dict:
    """Poll a GSI query until the expected count is reached or timeout."""
    deadline = time.monotonic() + timeout
    resp = None
    while time.monotonic() < deadline:
        resp = client.query(
            TableName=table_name,
            IndexName=index_name,
            KeyConditionExpression=key_expr,
            ExpressionAttributeValues=expr_values,
        )
        if resp["Count"] == expected:
            return resp
        time.sleep(0.05)
    return resp
@pytest.fixture()
def multipart_gsi_table(dynamodb_client, unique_table_name):
    """Create a table with a multi-part GSI (2 HASH + 1 RANGE)."""
    table_name = unique_table_name
    dynamodb_client.create_table(
        TableName=table_name,
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "gsi_pk1", "AttributeType": "S"},
            {"AttributeName": "gsi_pk2", "AttributeType": "S"},
            {"AttributeName": "gsi_sk", "AttributeType": "N"},
        ],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "multi-key-gsi",
                "KeySchema": [
                    {"AttributeName": "gsi_pk1", "KeyType": "HASH"},
                    {"AttributeName": "gsi_pk2", "KeyType": "HASH"},
                    {"AttributeName": "gsi_sk", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_active(dynamodb_client, table_name)
    yield table_name
    try:
        dynamodb_client.delete_table(TableName=table_name)
    except Exception:
        pass
    else:
        wait_for_deleted(dynamodb_client, table_name)
class TestMultipartGsiCreation:
    """Test table creation with multi-part GSI key schemas."""

    def test_create_table_with_2hash_1range_gsi(
        self, dynamodb_client, multipart_gsi_table
    ):
        """A GSI with 2 HASH + 1 RANGE key should be created successfully."""
        resp = dynamodb_client.describe_table(TableName=multipart_gsi_table)
        gsis = resp["Table"]["GlobalSecondaryIndexes"]
        assert len(gsis) == 1
        gsi = gsis[0]
        assert gsi["IndexName"] == "multi-key-gsi"
        assert len(gsi["KeySchema"]) == 3
        assert gsi["KeySchema"][0]["KeyType"] == "HASH"
        assert gsi["KeySchema"][1]["KeyType"] == "HASH"
        assert gsi["KeySchema"][2]["KeyType"] == "RANGE"

    def test_create_table_with_4hash_gsi(self, dynamodb_client, unique_table_name):
        """A GSI with 4 HASH keys (max) should be created successfully."""
        table_name = unique_table_name
        dynamodb_client.create_table(
            TableName=table_name,
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "a", "AttributeType": "S"},
                {"AttributeName": "b", "AttributeType": "S"},
                {"AttributeName": "c", "AttributeType": "S"},
                {"AttributeName": "d", "AttributeType": "S"},
            ],
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "four-hash-gsi",
                    "KeySchema": [
                        {"AttributeName": "a", "KeyType": "HASH"},
                        {"AttributeName": "b", "KeyType": "HASH"},
                        {"AttributeName": "c", "KeyType": "HASH"},
                        {"AttributeName": "d", "KeyType": "HASH"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                },
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        wait_for_active(dynamodb_client, table_name)
        try:
            resp = dynamodb_client.describe_table(TableName=table_name)
            gsi_ks = resp["Table"]["GlobalSecondaryIndexes"][0]["KeySchema"]
            assert len(gsi_ks) == 4
            assert all(k["KeyType"] == "HASH" for k in gsi_ks)
        finally:
            try:
                dynamodb_client.delete_table(TableName=table_name)
            except Exception:
                pass
            else:
                wait_for_deleted(dynamodb_client, table_name)

    def test_reject_5_hash_keys(self, dynamodb_client, unique_table_name):
        """A GSI with 5 HASH keys should be rejected."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.create_table(
                TableName=unique_table_name,
                AttributeDefinitions=[
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "a", "AttributeType": "S"},
                    {"AttributeName": "b", "AttributeType": "S"},
                    {"AttributeName": "c", "AttributeType": "S"},
                    {"AttributeName": "d", "AttributeType": "S"},
                    {"AttributeName": "e", "AttributeType": "S"},
                ],
                KeySchema=[
                    {"AttributeName": "pk", "KeyType": "HASH"},
                ],
                GlobalSecondaryIndexes=[
                    {
                        "IndexName": "too-many-gsi",
                        "KeySchema": [
                            {"AttributeName": "a", "KeyType": "HASH"},
                            {"AttributeName": "b", "KeyType": "HASH"},
                            {"AttributeName": "c", "KeyType": "HASH"},
                            {"AttributeName": "d", "KeyType": "HASH"},
                            {"AttributeName": "e", "KeyType": "HASH"},
                        ],
                        "Projection": {"ProjectionType": "ALL"},
                    },
                ],
                BillingMode="PAY_PER_REQUEST",
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"

    def test_reject_hash_after_range(self, dynamodb_client, unique_table_name):
        """A GSI with HASH after RANGE should be rejected."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.create_table(
                TableName=unique_table_name,
                AttributeDefinitions=[
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "a", "AttributeType": "S"},
                    {"AttributeName": "b", "AttributeType": "S"},
                    {"AttributeName": "c", "AttributeType": "S"},
                ],
                KeySchema=[
                    {"AttributeName": "pk", "KeyType": "HASH"},
                ],
                GlobalSecondaryIndexes=[
                    {
                        "IndexName": "bad-order-gsi",
                        "KeySchema": [
                            {"AttributeName": "a", "KeyType": "HASH"},
                            {"AttributeName": "b", "KeyType": "RANGE"},
                            {"AttributeName": "c", "KeyType": "HASH"},
                        ],
                        "Projection": {"ProjectionType": "ALL"},
                    },
                ],
                BillingMode="PAY_PER_REQUEST",
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
class TestMultipartGsiWriteAndQuery:
    """Test item writes and queries against multi-part GSI keys."""

    def test_put_and_query_multipart_gsi(
        self, dynamodb_client, multipart_gsi_table
    ):
        """Items written to the base table should be queryable via multi-part GSI."""
        table_name = multipart_gsi_table

        # Write items
        for i in range(5):
            dynamodb_client.put_item(
                TableName=table_name,
                Item={
                    "pk": {"S": f"item-{i}"},
                    "gsi_pk1": {"S": "tenant-A"},
                    "gsi_pk2": {"S": "region-us"},
                    "gsi_sk": {"N": str(i * 10)},
                    "data": {"S": f"value-{i}"},
                },
            )

        # Write items with different GSI partition
        for i in range(3):
            dynamodb_client.put_item(
                TableName=table_name,
                Item={
                    "pk": {"S": f"other-{i}"},
                    "gsi_pk1": {"S": "tenant-B"},
                    "gsi_pk2": {"S": "region-eu"},
                    "gsi_sk": {"N": str(i * 10)},
                    "data": {"S": f"other-{i}"},
                },
            )

        # Query the GSI with both HASH keys (poll for async GSI update).
        resp = wait_for_gsi_count(
            dynamodb_client, table_name, "multi-key-gsi",
            "gsi_pk1 = :pk1 AND gsi_pk2 = :pk2",
            {":pk1": {"S": "tenant-A"}, ":pk2": {"S": "region-us"}},
            expected=5,
        )
        assert resp["Count"] == 5

        # Query with SK range condition (poll for async GSI update).
        resp = wait_for_gsi_count(
            dynamodb_client, table_name, "multi-key-gsi",
            "gsi_pk1 = :pk1 AND gsi_pk2 = :pk2 AND gsi_sk > :sk",
            {":pk1": {"S": "tenant-A"}, ":pk2": {"S": "region-us"}, ":sk": {"N": "20"}},
            expected=2,
        )
        assert resp["Count"] == 2  # items with gsi_sk 30 and 40

    def test_item_without_gsi_keys_not_indexed(
        self, dynamodb_client, multipart_gsi_table
    ):
        """Items missing GSI key attributes should not appear in the GSI."""
        table_name = multipart_gsi_table

        # Write item without gsi_pk2
        dynamodb_client.put_item(
            TableName=table_name,
            Item={
                "pk": {"S": "no-gsi-pk2"},
                "gsi_pk1": {"S": "tenant-A"},
                "gsi_sk": {"N": "100"},
            },
        )

        # Query should return 0 items for this partition
        resp = dynamodb_client.query(
            TableName=table_name,
            IndexName="multi-key-gsi",
            KeyConditionExpression="gsi_pk1 = :pk1 AND gsi_pk2 = :pk2",
            ExpressionAttributeValues={
                ":pk1": {"S": "tenant-A"},
                ":pk2": {"S": "nonexistent"},
            },
        )
        assert resp["Count"] == 0

    def test_update_item_syncs_gsi(
        self, dynamodb_client, multipart_gsi_table
    ):
        """Updating an item's GSI key attributes should update the GSI."""
        table_name = multipart_gsi_table

        # Write initial item
        dynamodb_client.put_item(
            TableName=table_name,
            Item={
                "pk": {"S": "update-test"},
                "gsi_pk1": {"S": "old-tenant"},
                "gsi_pk2": {"S": "old-region"},
                "gsi_sk": {"N": "1"},
            },
        )

        # Update to new GSI partition
        dynamodb_client.put_item(
            TableName=table_name,
            Item={
                "pk": {"S": "update-test"},
                "gsi_pk1": {"S": "new-tenant"},
                "gsi_pk2": {"S": "new-region"},
                "gsi_sk": {"N": "1"},
            },
        )

        # Old partition should be empty (poll for async GSI update).
        resp = wait_for_gsi_count(
            dynamodb_client, table_name, "multi-key-gsi",
            "gsi_pk1 = :pk1 AND gsi_pk2 = :pk2",
            {":pk1": {"S": "old-tenant"}, ":pk2": {"S": "old-region"}},
            expected=0,
        )
        assert resp["Count"] == 0

        # New partition should have the item (poll for async GSI update).
        resp = wait_for_gsi_count(
            dynamodb_client, table_name, "multi-key-gsi",
            "gsi_pk1 = :pk1 AND gsi_pk2 = :pk2",
            {":pk1": {"S": "new-tenant"}, ":pk2": {"S": "new-region"}},
            expected=1,
        )
        assert resp["Count"] == 1

    def test_delete_item_removes_from_gsi(
        self, dynamodb_client, multipart_gsi_table
    ):
        """Deleting an item should remove it from the multi-part GSI."""
        table_name = multipart_gsi_table

        dynamodb_client.put_item(
            TableName=table_name,
            Item={
                "pk": {"S": "delete-test"},
                "gsi_pk1": {"S": "del-tenant"},
                "gsi_pk2": {"S": "del-region"},
                "gsi_sk": {"N": "99"},
            },
        )

        dynamodb_client.delete_item(
            TableName=table_name,
            Key={"pk": {"S": "delete-test"}},
        )

        # Poll for async GSI update after delete.
        resp = wait_for_gsi_count(
            dynamodb_client, table_name, "multi-key-gsi",
            "gsi_pk1 = :pk1 AND gsi_pk2 = :pk2",
            {":pk1": {"S": "del-tenant"}, ":pk2": {"S": "del-region"}},
            expected=0,
        )
        assert resp["Count"] == 0

    def test_query_with_reversed_attribute_order(
        self, dynamodb_client, multipart_gsi_table
    ):
        """Query should work regardless of attribute order in KeyConditionExpression."""
        table_name = multipart_gsi_table

        dynamodb_client.put_item(
            TableName=table_name,
            Item={
                "pk": {"S": "order-test"},
                "gsi_pk1": {"S": "tenant-X"},
                "gsi_pk2": {"S": "region-ap"},
                "gsi_sk": {"N": "42"},
                "data": {"S": "found-it"},
            },
        )

        # Query with reversed HASH attribute order (pk2 before pk1).
        # Poll for async GSI update.
        resp = wait_for_gsi_count(
            dynamodb_client, table_name, "multi-key-gsi",
            "gsi_pk2 = :pk2 AND gsi_pk1 = :pk1",
            {":pk1": {"S": "tenant-X"}, ":pk2": {"S": "region-ap"}},
            expected=1,
        )
        assert resp["Count"] == 1
        assert resp["Items"][0]["data"]["S"] == "found-it"

    def test_query_missing_hash_attribute_rejected(
        self, dynamodb_client, multipart_gsi_table
    ):
        """Query with incomplete HASH attributes should return ValidationException."""
        from botocore.exceptions import ClientError

        table_name = multipart_gsi_table

        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.query(
                TableName=table_name,
                IndexName="multi-key-gsi",
                KeyConditionExpression="gsi_pk1 = :pk1",
                ExpressionAttributeValues={
                    ":pk1": {"S": "tenant-A"},
                },
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert "gsi_pk2" in err["Message"]
