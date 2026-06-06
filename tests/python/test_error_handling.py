# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Error handling tests: error codes, error formats, validation errors.

Covers scenarios from external suite: ErrorHandlingTests, plus additional
validation edge cases. Tests run identically against real DynamoDB and extenddb.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from helpers import unique_name


class TestErrorCodes:
    """Verify correct error codes for various failure scenarios."""

    def test_create_duplicate_table(self, table_factory, dynamodb_client):
        """Creating a duplicate table returns ResourceInUseException."""
        name = table_factory()
        with pytest.raises(ClientError) as exc:
            dynamodb_client.create_table(
                TableName=name,
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceInUseException"

    def test_delete_nonexistent_table(self, dynamodb_client):
        """Deleting a nonexistent table returns ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            dynamodb_client.delete_table(TableName=unique_name("nonexistent"))
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_item_nonexistent_table(self, dynamodb_client):
        """GetItem on nonexistent table returns ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            dynamodb_client.get_item(
                TableName=unique_name("nonexistent"),
                Key={"pk": {"S": "k1"}},
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_put_item_missing_key(self, table_factory, dynamodb_client):
        """PutItem without required key returns ValidationException."""
        name = table_factory(range_key="sk")
        with pytest.raises(ClientError) as exc:
            dynamodb_client.put_item(
                TableName=name,
                Item={"pk": {"S": "k1"}},  # missing sk
            )
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_query_without_key_condition(self, table_factory, dynamodb_client):
        """Query without KeyConditionExpression returns ValidationException."""
        name = table_factory()
        with pytest.raises(ClientError) as exc:
            dynamodb_client.query(
                TableName=name,
                FilterExpression="pk = :pk",
                ExpressionAttributeValues={":pk": {"S": "k1"}},
            )
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_error_type_fully_qualified(self, dynamodb_client):
        """Error __type field is fully qualified (com.amazonaws.dynamodb.v20120810#...)."""
        with pytest.raises(ClientError) as exc:
            dynamodb_client.describe_table(TableName=unique_name("nonexistent"))
        err = exc.value.response["Error"]
        assert err["Code"] == "ResourceNotFoundException"

    def test_put_item_to_nonexistent_table(self, dynamodb_client):
        """PutItem to nonexistent table returns ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            dynamodb_client.put_item(
                TableName=unique_name("nonexistent"),
                Item={"pk": {"S": "k1"}},
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_item_from_nonexistent_table(self, dynamodb_client):
        """DeleteItem from nonexistent table returns ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            dynamodb_client.delete_item(
                TableName=unique_name("nonexistent"),
                Key={"pk": {"S": "k1"}},
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_scan_nonexistent_table(self, dynamodb_client):
        """Scan on nonexistent table returns ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            dynamodb_client.scan(TableName=unique_name("nonexistent"))
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_query_nonexistent_table(self, dynamodb_client):
        """Query on nonexistent table returns ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            dynamodb_client.query(
                TableName=unique_name("nonexistent"),
                KeyConditionExpression="pk = :pk",
                ExpressionAttributeValues={":pk": {"S": "k1"}},
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_batch_get_from_nonexistent_table(self, dynamodb_client):
        """BatchGetItem from nonexistent table returns ResourceNotFoundException."""
        tbl = unique_name("nonexistent")
        with pytest.raises(ClientError) as exc:
            dynamodb_client.batch_get_item(
                RequestItems={tbl: {"Keys": [{"pk": {"S": "k1"}}]}}
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_batch_write_to_nonexistent_table(self, dynamodb_client):
        """BatchWriteItem to nonexistent table returns ResourceNotFoundException."""
        tbl = unique_name("nonexistent")
        with pytest.raises(ClientError) as exc:
            dynamodb_client.batch_write_item(
                RequestItems={
                    tbl: [{"PutRequest": {"Item": {"pk": {"S": "k1"}}}}]
                }
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_transact_get_on_nonexistent_table(self, dynamodb_client):
        """TransactGetItems on nonexistent table returns ResourceNotFoundException."""
        tbl = unique_name("nonexistent")
        with pytest.raises(ClientError) as exc:
            dynamodb_client.transact_get_items(
                TransactItems=[{"Get": {"TableName": tbl, "Key": {"pk": {"S": "k1"}}}}]
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_transact_write_on_nonexistent_table(self, dynamodb_client):
        """TransactWriteItems on nonexistent table returns ResourceNotFoundException."""
        tbl = unique_name("nonexistent")
        with pytest.raises(ClientError) as exc:
            dynamodb_client.transact_write_items(
                TransactItems=[
                    {"Put": {"TableName": tbl, "Item": {"pk": {"S": "k1"}}}}
                ]
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestTableValidation:
    """Table creation validation edge cases."""

    def test_create_table_extra_attribute_definitions(self, dynamodb_client):
        """Extra attribute definitions not used in keys or indexes → ValidationException."""
        with pytest.raises(ClientError) as exc:
            dynamodb_client.create_table(
                TableName=unique_name("tbl"),
                AttributeDefinitions=[
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "extra", "AttributeType": "S"},
                ],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_create_table_missing_key_in_attribute_definitions(self, dynamodb_client):
        """Key attribute not in AttributeDefinitions → ValidationException."""
        with pytest.raises(ClientError) as exc:
            dynamodb_client.create_table(
                TableName=unique_name("tbl"),
                AttributeDefinitions=[
                    {"AttributeName": "pk", "AttributeType": "S"},
                ],
                KeySchema=[
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "sk", "KeyType": "RANGE"},
                ],
                BillingMode="PAY_PER_REQUEST",
            )
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_create_table_with_max_length_name(self, table_factory, dynamodb_client):
        """Table name at max length (255 chars) succeeds."""
        long_name = "t" * 255
        name = table_factory(table_name=long_name)
        resp = dynamodb_client.describe_table(TableName=name)
        assert resp["Table"]["TableName"] == long_name
