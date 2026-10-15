"""In-process regressions for FilterLogEvents ordering across streams."""

import json
import time
import uuid

import pytest

from ministack.services import cloudwatch_logs as logs


@pytest.fixture
def log_group():
    group = f"/test/filter-pattern/{uuid.uuid4().hex}"
    assert logs._create_log_group({"logGroupName": group})[0] == 200
    yield group
    assert logs._delete_log_group({"logGroupName": group})[0] == 200


def put_messages(group, stream, messages):
    assert logs._create_log_stream({
        "logGroupName": group, "logStreamName": stream,
    })[0] == 200
    response = logs._put_log_events({
        "logGroupName": group,
        "logStreamName": stream,
        "logEvents": [{"timestamp": timestamp, "message": message}
                      for timestamp, message in messages],
    })
    assert response[0] == 200


def test_filter_applies_limit_after_global_stream_order(log_group):
    now = int(time.time() * 1000)
    # The stream scanned first has newer events than the second stream.
    put_messages(log_group, "newer", [(now + 30, "match newer")])
    put_messages(log_group, "older", [
        (now, "unrelated"), (now + 10, "match first"), (now + 20, "match second"),
    ])
    response = logs._filter_log_events({
        "logGroupName": log_group, "filterPattern": "match", "limit": 1,
    })
    assert response[0] == 200
    assert [e["message"] for e in json.loads(response[2])["events"]] == ["match first"]


def test_filter_does_not_stop_later_stream_after_partial_global_limit(log_group):
    now = int(time.time() * 1000)
    put_messages(log_group, "newer", [(now + 30, "match newer")])
    put_messages(log_group, "older", [(now + 10, "match first"), (now + 20, "match second")])
    response = logs._filter_log_events({
        "logGroupName": log_group, "filterPattern": "match", "limit": 2,
    })
    assert response[0] == 200
    assert [e["message"] for e in json.loads(response[2])["events"]] == [
        "match first", "match second",
    ]
