# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Data type tests: all DynamoDB types, empty values, unicode, nested structures.

Covers scenarios from external suite: DataTypeTests, EmptyValueTests, UnicodeTests.
Tests run identically against real DynamoDB and extenddb.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError, ParamValidationError


class TestAllDataTypes:
    """Verify every DynamoDB data type round-trips correctly."""

    def test_string_type(self, table_factory, dynamodb_client):
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "val": {"S": "hello world"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["val"]["S"] == "hello world"

    def test_number_type(self, table_factory, dynamodb_client):
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "val": {"N": "3.14159"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["val"]["N"] == "3.14159"

    def test_binary_type(self, table_factory, dynamodb_client):
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "val": {"B": b"\xde\xad\xbe\xef"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["val"]["B"] == b"\xde\xad\xbe\xef"

    def test_boolean_type(self, table_factory, dynamodb_client):
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "t": {"BOOL": True}, "f": {"BOOL": False}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["t"]["BOOL"] is True
        assert resp["Item"]["f"]["BOOL"] is False

    def test_null_type(self, table_factory, dynamodb_client):
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "val": {"NULL": True}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["val"]["NULL"] is True

    def test_string_set_type(self, table_factory, dynamodb_client):
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "val": {"SS": ["a", "b", "c"]}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert set(resp["Item"]["val"]["SS"]) == {"a", "b", "c"}

    def test_number_set_type(self, table_factory, dynamodb_client):
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "val": {"NS": ["1", "2.5", "3"]}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert set(resp["Item"]["val"]["NS"]) == {"1", "2.5", "3"}

    def test_list_type(self, table_factory, dynamodb_client):
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "val": {"L": [{"S": "a"}, {"N": "1"}]}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        lst = resp["Item"]["val"]["L"]
        assert lst[0]["S"] == "a"
        assert lst[1]["N"] == "1"

    def test_map_type(self, table_factory, dynamodb_client):
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "val": {"M": {"k": {"S": "v"}, "n": {"N": "9"}}}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        m = resp["Item"]["val"]["M"]
        assert m["k"]["S"] == "v"
        assert m["n"]["N"] == "9"

    def test_nested_map_and_list(self, table_factory, dynamodb_client):
        """Deeply nested map containing lists and vice versa."""
        name = table_factory()
        item = {
            "pk": {"S": "k1"},
            "nested": {
                "M": {
                    "level1": {
                        "L": [
                            {"M": {"level2": {"S": "deep"}}},
                            {"L": [{"N": "42"}]},
                        ]
                    }
                }
            },
        }
        dynamodb_client.put_item(TableName=name, Item=item)
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        nested = resp["Item"]["nested"]["M"]["level1"]["L"]
        assert nested[0]["M"]["level2"]["S"] == "deep"
        assert nested[1]["L"][0]["N"] == "42"

    def test_all_types_in_one_item(self, table_factory, dynamodb_client):
        """An item containing every DynamoDB data type."""
        name = table_factory()
        item = {
            "pk": {"S": "all-types"},
            "str_attr": {"S": "hello"},
            "num_attr": {"N": "42"},
            "bin_attr": {"B": b"\x01\x02"},
            "bool_attr": {"BOOL": True},
            "null_attr": {"NULL": True},
            "ss_attr": {"SS": ["x", "y"]},
            "ns_attr": {"NS": ["1", "2"]},
            "bs_attr": {"BS": [b"\x01", b"\x02"]},
            "list_attr": {"L": [{"S": "a"}, {"N": "1"}]},
            "map_attr": {"M": {"k": {"S": "v"}}},
        }
        dynamodb_client.put_item(TableName=name, Item=item)
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "all-types"}})
        got = resp["Item"]
        assert got["str_attr"]["S"] == "hello"
        assert got["num_attr"]["N"] == "42"
        assert got["bin_attr"]["B"] == b"\x01\x02"
        assert got["bool_attr"]["BOOL"] is True
        assert got["null_attr"]["NULL"] is True
        assert set(got["ss_attr"]["SS"]) == {"x", "y"}
        assert set(got["ns_attr"]["NS"]) == {"1", "2"}
        assert len(got["bs_attr"]["BS"]) == 2
        assert got["list_attr"]["L"][0]["S"] == "a"
        assert got["map_attr"]["M"]["k"]["S"] == "v"


class TestEmptyValues:
    """Empty string, empty binary, and empty set handling."""

    def test_put_item_empty_string_non_key(self, table_factory, dynamodb_client):
        """Empty string is allowed for non-key attributes."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "empty": {"S": ""}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["empty"]["S"] == ""

    def test_put_item_empty_binary_non_key(self, table_factory, dynamodb_client):
        """Empty binary is allowed for non-key attributes."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "empty": {"B": b""}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["empty"]["B"] == b""

    def test_put_item_empty_string_set(self, table_factory, dynamodb_client):
        """Empty string set is rejected by DynamoDB (or boto3 client-side)."""
        name = table_factory()
        with pytest.raises((ClientError, ParamValidationError)):
            dynamodb_client.put_item(
                TableName=name,
                Item={"pk": {"S": "k1"}, "tags": {"SS": []}},
            )

    def test_get_item_returns_empty_string(self, table_factory, dynamodb_client):
        """GetItem returns empty string attributes faithfully."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "a": {"S": ""}, "b": {"S": "notempty"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["a"]["S"] == ""
        assert resp["Item"]["b"]["S"] == "notempty"

    def test_batch_write_with_empty_string(self, table_factory, dynamodb_client):
        """BatchWriteItem with empty string attribute."""
        name = table_factory()
        dynamodb_client.batch_write_item(
            RequestItems={
                name: [
                    {"PutRequest": {"Item": {"pk": {"S": "k1"}, "val": {"S": ""}}}},
                ]
            }
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["val"]["S"] == ""

    def test_update_item_set_empty_string(self, table_factory, dynamodb_client):
        """UpdateItem can SET an attribute to empty string."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "val": {"S": "notempty"}},
        )
        dynamodb_client.update_item(
            TableName=name,
            Key={"pk": {"S": "k1"}},
            UpdateExpression="SET val = :e",
            ExpressionAttributeValues={":e": {"S": ""}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["val"]["S"] == ""

    def test_scan_returns_items_with_empty_strings(self, table_factory, dynamodb_client):
        """Scan returns items that have empty string attributes."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "val": {"S": ""}},
        )
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k2"}, "val": {"S": "notempty"}},
        )
        resp = dynamodb_client.scan(TableName=name)
        items = {i["pk"]["S"]: i for i in resp["Items"]}
        assert items["k1"]["val"]["S"] == ""
        assert items["k2"]["val"]["S"] == "notempty"

    def test_transact_write_with_empty_string(self, table_factory, dynamodb_client):
        """TransactWriteItems with empty string attribute."""
        name = table_factory()
        dynamodb_client.transact_write_items(
            TransactItems=[
                {
                    "Put": {
                        "TableName": name,
                        "Item": {"pk": {"S": "k1"}, "val": {"S": ""}},
                    }
                }
            ]
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["val"]["S"] == ""


class TestUnicode:
    """Unicode handling in attribute names and values."""

    def test_unicode_string_values(self, table_factory, dynamodb_client):
        """Unicode characters in string values."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "val": {"S": "日本語テスト"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["val"]["S"] == "日本語テスト"

    def test_emoji(self, table_factory, dynamodb_client):
        """Emoji characters in values."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "val": {"S": "🎉🚀💯"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["val"]["S"] == "🎉🚀💯"

    def test_unicode_in_attribute_names(self, table_factory, dynamodb_client):
        """Unicode characters in attribute names."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "名前": {"S": "太郎"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["名前"]["S"] == "太郎"

    def test_special_characters_in_values(self, table_factory, dynamodb_client):
        """Special characters: quotes, backslashes, newlines."""
        name = table_factory()
        val = 'He said "hello"\nand\\then\ttabs'
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "val": {"S": val}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["val"]["S"] == val

    def test_single_quote_in_values(self, table_factory, dynamodb_client):
        """Single quotes in values."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "val": {"S": "it's a test"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["val"]["S"] == "it's a test"

    def test_long_unicode_string(self, table_factory, dynamodb_client):
        """Long unicode string (multi-byte characters)."""
        name = table_factory()
        val = "あ" * 1000
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "val": {"S": val}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["val"]["S"] == val

    def test_empty_string_attribute(self, table_factory, dynamodb_client):
        """Empty string is a valid non-key attribute value."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "val": {"S": ""}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["val"]["S"] == ""

    def test_unicode_in_query_filter(self, table_factory, dynamodb_client):
        """Unicode in filter expression values."""
        name = table_factory(range_key="sk")
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "p1"}, "sk": {"S": "s1"}, "city": {"S": "東京"}},
        )
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "p1"}, "sk": {"S": "s2"}, "city": {"S": "大阪"}},
        )
        resp = dynamodb_client.query(
            TableName=name,
            KeyConditionExpression="pk = :pk",
            FilterExpression="city = :city",
            ExpressionAttributeValues={
                ":pk": {"S": "p1"},
                ":city": {"S": "東京"},
            },
        )
        assert resp["Count"] == 1
        assert resp["Items"][0]["city"]["S"] == "東京"


class TestNestedStructures:
    """Nested empty and non-empty list/map structures."""

    def test_nested_empty_list(self, table_factory, dynamodb_client):
        """Nested attribute containing an empty list."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "data": {"M": {"items": {"L": []}}}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["data"]["M"]["items"]["L"] == []

    def test_nested_empty_map(self, table_factory, dynamodb_client):
        """Nested attribute containing an empty map."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "data": {"M": {"meta": {"M": {}}}}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["data"]["M"]["meta"]["M"] == {}

    def test_nested_non_empty_list(self, table_factory, dynamodb_client):
        """Nested list with mixed types."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={
                "pk": {"S": "k1"},
                "data": {
                    "M": {
                        "items": {
                            "L": [
                                {"S": "text"},
                                {"N": "42"},
                                {"BOOL": True},
                                {"NULL": True},
                                {"M": {"nested": {"S": "val"}}},
                            ]
                        }
                    }
                },
            },
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        items = resp["Item"]["data"]["M"]["items"]["L"]
        assert len(items) == 5
        assert items[0]["S"] == "text"
        assert items[4]["M"]["nested"]["S"] == "val"

    def test_nested_non_empty_map(self, table_factory, dynamodb_client):
        """Nested map with multiple levels."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={
                "pk": {"S": "k1"},
                "data": {
                    "M": {
                        "level1": {
                            "M": {
                                "level2": {
                                    "M": {"value": {"S": "deep"}}
                                }
                            }
                        }
                    }
                },
            },
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        deep = resp["Item"]["data"]["M"]["level1"]["M"]["level2"]["M"]["value"]["S"]
        assert deep == "deep"

    def test_attribute_type_change(self, table_factory, dynamodb_client):
        """Overwriting an attribute changes its type."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "val": {"S": "string"}},
        )
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "val": {"N": "42"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert "N" in resp["Item"]["val"]
        assert resp["Item"]["val"]["N"] == "42"
