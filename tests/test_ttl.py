# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Tests for the TTL (Time To Live) subsystem."""

from __future__ import annotations

import os
import time
import uuid

import boto3
import pytest
from botocore.exceptions import ClientError

from conftest import wait_for_active, wait_for_deleted


@pytest.fixture()
def ttl_table(dynamodb_client, unique_table_name):
    """Create a PAY_PER_REQUEST table for TTL tests."""
    dynamodb_client.create_table(
        TableName=unique_table_name,
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_active(dynamodb_client, unique_table_name)
    yield unique_table_name
    try:
        dynamodb_client.delete_table(TableName=unique_table_name)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
    else:
        wait_for_deleted(dynamodb_client, unique_table_name)


class TestUpdateTimeToLive:
    """UpdateTimeToLive operation tests."""

    def test_enable_ttl(self, dynamodb_client, ttl_table):
        resp = dynamodb_client.update_time_to_live(
            TableName=ttl_table,
            TimeToLiveSpecification={
                "Enabled": True,
                "AttributeName": "expiry",
            },
        )
        spec = resp["TimeToLiveSpecification"]
        assert spec["Enabled"] is True
        assert spec["AttributeName"] == "expiry"

    def test_disable_ttl(self, dynamodb_client, ttl_table):
        # Enable first.
        dynamodb_client.update_time_to_live(
            TableName=ttl_table,
            TimeToLiveSpecification={
                "Enabled": True,
                "AttributeName": "expiry",
            },
        )
        # DynamoDB rate-limits TTL modifications; wait before disabling.
        time.sleep(6)
        # Disable.
        resp = dynamodb_client.update_time_to_live(
            TableName=ttl_table,
            TimeToLiveSpecification={
                "Enabled": False,
                "AttributeName": "expiry",
            },
        )
        spec = resp["TimeToLiveSpecification"]
        assert spec["Enabled"] is False

    def test_enable_ttl_nonexistent_table(self, dynamodb_client):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_time_to_live(
                TableName="nonexistent-table-xyz-999",
                TimeToLiveSpecification={
                    "Enabled": True,
                    "AttributeName": "expiry",
                },
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestDescribeTimeToLive:
    """DescribeTimeToLive operation tests."""

    def test_describe_ttl_disabled(self, dynamodb_client, ttl_table):
        resp = dynamodb_client.describe_time_to_live(TableName=ttl_table)
        desc = resp["TimeToLiveDescription"]
        assert desc["TimeToLiveStatus"] == "DISABLED"

    def test_describe_ttl_enabled(self, dynamodb_client, ttl_table):
        dynamodb_client.update_time_to_live(
            TableName=ttl_table,
            TimeToLiveSpecification={
                "Enabled": True,
                "AttributeName": "expiry",
            },
        )
        resp = dynamodb_client.describe_time_to_live(TableName=ttl_table)
        desc = resp["TimeToLiveDescription"]
        assert desc["TimeToLiveStatus"] in ("ENABLED", "ENABLING")
        assert desc.get("AttributeName") == "expiry"

    def test_describe_ttl_nonexistent_table(self, dynamodb_client):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.describe_time_to_live(
                TableName="nonexistent-table-xyz-999"
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestTtlExpiry:
    """TTL item expiry tests."""

    @pytest.mark.slow
    def test_expired_item_deleted(self, dynamodb_client, ttl_table):
        dynamodb_client.update_time_to_live(
            TableName=ttl_table,
            TimeToLiveSpecification={
                "Enabled": True,
                "AttributeName": "expiry",
            },
        )
        past_epoch = int(time.time()) - 3600
        dynamodb_client.put_item(
            TableName=ttl_table,
            Item={
                "pk": {"S": "ttl-test-1"},
                "expiry": {"N": str(past_epoch)},
                "data": {"S": "should-be-deleted"},
            },
        )

        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            resp = dynamodb_client.get_item(
                TableName=ttl_table, Key={"pk": {"S": "ttl-test-1"}}
            )
            if "Item" not in resp:
                return
            time.sleep(5)
        pytest.fail("TTL worker did not delete expired item within 90 seconds")

    @pytest.mark.slow
    def test_non_expired_item_not_deleted(self, dynamodb_client, ttl_table):
        dynamodb_client.update_time_to_live(
            TableName=ttl_table,
            TimeToLiveSpecification={
                "Enabled": True,
                "AttributeName": "expiry",
            },
        )
        dynamodb_client.put_item(
            TableName=ttl_table,
            Item={
                "pk": {"S": "ttl-future"},
                "expiry": {"N": str(int(time.time()) + 86_400)},
            },
        )

        time.sleep(5)
        resp = dynamodb_client.get_item(
            TableName=ttl_table, Key={"pk": {"S": "ttl-future"}}
        )
        assert "Item" in resp

    def test_item_without_ttl_attribute_not_deleted(self, dynamodb_client, ttl_table):
        dynamodb_client.update_time_to_live(
            TableName=ttl_table,
            TimeToLiveSpecification={
                "Enabled": True,
                "AttributeName": "expiry",
            },
        )
        dynamodb_client.put_item(
            TableName=ttl_table,
            Item={
                "pk": {"S": "no-ttl-attr"},
                "data": {"S": "should-persist"},
            },
        )

        time.sleep(5)
        resp = dynamodb_client.get_item(
            TableName=ttl_table, Key={"pk": {"S": "no-ttl-attr"}}
        )
        assert resp["Item"]["data"]["S"] == "should-persist"

    @pytest.mark.slow
    def test_ttl_expiry_generates_stream_record(self, dynamodb_client):
        endpoint_url = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
        kwargs: dict = dict(
            service_name="dynamodbstreams",
            region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
            endpoint_url=endpoint_url,
        )
        if endpoint_url.startswith("https://"):
            kwargs["verify"] = False
        streams_client = boto3.client(**kwargs)
        table_name = f"extenddb-ttl-stream-{uuid.uuid4().hex[:8]}"
        try:
            dynamodb_client.create_table(
                TableName=table_name,
                AttributeDefinitions=[
                    {"AttributeName": "pk", "AttributeType": "S"},
                ],
                KeySchema=[
                    {"AttributeName": "pk", "KeyType": "HASH"},
                ],
                BillingMode="PAY_PER_REQUEST",
                StreamSpecification={
                    "StreamEnabled": True,
                    "StreamViewType": "NEW_AND_OLD_IMAGES",
                },
            )
            wait_for_active(dynamodb_client, table_name)

            dynamodb_client.update_time_to_live(
                TableName=table_name,
                TimeToLiveSpecification={
                    "Enabled": True,
                    "AttributeName": "expiry",
                },
            )

            past_epoch = int(time.time()) - 3600
            dynamodb_client.put_item(
                TableName=table_name,
                Item={
                    "pk": {"S": "ttl-stream-test"},
                    "expiry": {"N": str(past_epoch)},
                    "data": {"S": "will-expire"},
                },
            )

            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                resp = dynamodb_client.get_item(
                    TableName=table_name, Key={"pk": {"S": "ttl-stream-test"}}
                )
                if "Item" not in resp:
                    break
                time.sleep(5)
            else:
                pytest.fail("TTL worker did not delete expired item within 90 seconds")

            desc = dynamodb_client.describe_table(TableName=table_name)
            stream_arn = desc["Table"].get("LatestStreamArn")
            assert stream_arn, "Table should have a stream ARN"

            stream_desc = streams_client.describe_stream(StreamArn=stream_arn)
            shards = stream_desc["StreamDescription"]["Shards"]
            assert shards, "Stream should have at least one shard"

            ttl_remove_record = None
            for shard in shards:
                iter_resp = streams_client.get_shard_iterator(
                    StreamArn=stream_arn,
                    ShardId=shard["ShardId"],
                    ShardIteratorType="TRIM_HORIZON",
                )
                shard_iter = iter_resp["ShardIterator"]

                for _ in range(3):
                    if not shard_iter:
                        break
                    records_resp = streams_client.get_records(
                        ShardIterator=shard_iter, Limit=100
                    )
                    for record in records_resp.get("Records", []):
                        keys = record["dynamodb"].get("Keys", {})
                        if (
                            record["eventName"] == "REMOVE"
                            and keys.get("pk", {}).get("S") == "ttl-stream-test"
                        ):
                            ttl_remove_record = record
                            break
                    if ttl_remove_record:
                        break
                    shard_iter = records_resp.get("NextShardIterator")
                if ttl_remove_record:
                    break

            assert ttl_remove_record is not None
            identity = ttl_remove_record.get("userIdentity")
            assert identity is not None
            assert identity.get("Type") == "Service"
            assert identity.get("PrincipalId") == "dynamodb.amazonaws.com"
            old_image = ttl_remove_record["dynamodb"].get("OldImage")
            assert old_image is not None
            assert old_image.get("pk", {}).get("S") == "ttl-stream-test"
        finally:
            try:
                dynamodb_client.delete_table(TableName=table_name)
                wait_for_deleted(dynamodb_client, table_name)
            except Exception:
                pass
