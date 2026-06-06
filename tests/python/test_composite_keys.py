# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Composite key tests: hash+range operations, different key types, GSI on composite tables.

Covers scenarios from external suite: CompositeKeyTests.
Tests run identically against real DynamoDB and extenddb.
"""

from __future__ import annotations


class TestCompositeKeyOperations:
    """Operations on tables with hash + range keys."""

    def test_put_and_get_with_composite_key(self, table_factory, dynamodb_client):
        """Put and get with hash + range key."""
        name = table_factory(range_key="sk")
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "p1"}, "sk": {"S": "s1"}, "data": {"S": "val"}},
        )
        resp = dynamodb_client.get_item(
            TableName=name, Key={"pk": {"S": "p1"}, "sk": {"S": "s1"}}
        )
        assert resp["Item"]["data"]["S"] == "val"

    def test_delete_with_composite_key(self, table_factory, dynamodb_client):
        """Delete with composite key removes only the targeted item."""
        name = table_factory(range_key="sk")
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "p1"}, "sk": {"S": "s1"}, "data": {"S": "v1"}},
        )
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "p1"}, "sk": {"S": "s2"}, "data": {"S": "v2"}},
        )
        dynamodb_client.delete_item(
            TableName=name, Key={"pk": {"S": "p1"}, "sk": {"S": "s1"}}
        )
        resp = dynamodb_client.get_item(
            TableName=name, Key={"pk": {"S": "p1"}, "sk": {"S": "s1"}}
        )
        assert "Item" not in resp
        resp = dynamodb_client.get_item(
            TableName=name, Key={"pk": {"S": "p1"}, "sk": {"S": "s2"}}
        )
        assert resp["Item"]["data"]["S"] == "v2"

    def test_delete_with_wrong_range_key(self, table_factory, dynamodb_client):
        """Delete with non-matching range key does nothing."""
        name = table_factory(range_key="sk")
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "p1"}, "sk": {"S": "s1"}, "data": {"S": "v1"}},
        )
        dynamodb_client.delete_item(
            TableName=name, Key={"pk": {"S": "p1"}, "sk": {"S": "wrong"}}
        )
        resp = dynamodb_client.get_item(
            TableName=name, Key={"pk": {"S": "p1"}, "sk": {"S": "s1"}}
        )
        assert resp["Item"]["data"]["S"] == "v1"

    def test_query_by_hash_key(self, table_factory, dynamodb_client):
        """Query returns all items for a partition key."""
        name = table_factory(range_key="sk")
        for i in range(5):
            dynamodb_client.put_item(
                TableName=name,
                Item={"pk": {"S": "p1"}, "sk": {"S": f"s{i}"}, "i": {"N": str(i)}},
            )
        resp = dynamodb_client.query(
            TableName=name,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "p1"}},
        )
        assert resp["Count"] == 5

    def test_query_with_range_key_condition(self, table_factory, dynamodb_client):
        """Query with range key condition filters results."""
        name = table_factory(range_key="sk")
        for i in range(5):
            dynamodb_client.put_item(
                TableName=name,
                Item={"pk": {"S": "p1"}, "sk": {"S": f"item-{i:03d}"}},
            )
        resp = dynamodb_client.query(
            TableName=name,
            KeyConditionExpression="pk = :pk AND sk BETWEEN :lo AND :hi",
            ExpressionAttributeValues={
                ":pk": {"S": "p1"},
                ":lo": {"S": "item-001"},
                ":hi": {"S": "item-003"},
            },
        )
        assert resp["Count"] == 3

    def test_query_scan_forward(self, table_factory, dynamodb_client):
        """Query with ScanIndexForward=False returns descending order."""
        name = table_factory(range_key="sk")
        for i in range(3):
            dynamodb_client.put_item(
                TableName=name,
                Item={"pk": {"S": "p1"}, "sk": {"S": f"s{i}"}},
            )
        resp = dynamodb_client.query(
            TableName=name,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "p1"}},
            ScanIndexForward=False,
        )
        sks = [item["sk"]["S"] for item in resp["Items"]]
        assert sks == sorted(sks, reverse=True)

    def test_query_with_limit(self, table_factory, dynamodb_client):
        """Query with Limit returns at most Limit items."""
        name = table_factory(range_key="sk")
        for i in range(5):
            dynamodb_client.put_item(
                TableName=name,
                Item={"pk": {"S": "p1"}, "sk": {"S": f"s{i}"}},
            )
        resp = dynamodb_client.query(
            TableName=name,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "p1"}},
            Limit=2,
        )
        assert len(resp["Items"]) == 2
        assert "LastEvaluatedKey" in resp

    def test_query_pagination(self, table_factory, dynamodb_client):
        """Paginate through all items using ExclusiveStartKey."""
        name = table_factory(range_key="sk")
        for i in range(5):
            dynamodb_client.put_item(
                TableName=name,
                Item={"pk": {"S": "p1"}, "sk": {"S": f"s{i}"}},
            )
        all_items = []
        kwargs = {
            "TableName": name,
            "KeyConditionExpression": "pk = :pk",
            "ExpressionAttributeValues": {":pk": {"S": "p1"}},
            "Limit": 2,
        }
        while True:
            resp = dynamodb_client.query(**kwargs)
            all_items.extend(resp["Items"])
            if "LastEvaluatedKey" not in resp:
                break
            kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
        assert len(all_items) == 5


class TestNumberHashKey:
    """Tables with number hash keys."""

    def test_number_hash_key(self, table_factory, dynamodb_client):
        """Put and get with number hash key."""
        name = table_factory(hash_key="id", hash_type="N")
        dynamodb_client.put_item(
            TableName=name,
            Item={"id": {"N": "42"}, "data": {"S": "val"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"id": {"N": "42"}})
        assert resp["Item"]["data"]["S"] == "val"


class TestBlobHashKey:
    """Tables with binary hash keys."""

    def test_blob_hash_key(self, table_factory, dynamodb_client):
        """Put and get with binary hash key."""
        name = table_factory(hash_key="bk", hash_type="B")
        dynamodb_client.put_item(
            TableName=name,
            Item={"bk": {"B": b"\x01\x02\x03"}, "data": {"S": "val"}},
        )
        resp = dynamodb_client.get_item(
            TableName=name, Key={"bk": {"B": b"\x01\x02\x03"}}
        )
        assert resp["Item"]["data"]["S"] == "val"
