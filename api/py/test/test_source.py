#     Copyright (C) 2023 The Chronon Authors.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

import ai.chronon.query as query
import pytest

from ai.chronon.api import ttypes
from ai.chronon.source import validate_source

_EVENT_SOURCE = ttypes.EventSource(
    table="events_table",
    query=query.Query(
        selects=None,
        time_column="ts"
    )
)

_ENTITY_SOURCE = ttypes.EntitySource(
    snapshotTable="entity_table",
    query=query.Query(
        selects=None,
        time_column="ts"
    )
)

_JOIN_SOURCE = ttypes.JoinSource(
    join=ttypes.Join(),
    query=query.Query(
        selects=None,
        time_column="ts"
    )
)


def test_valid_event_source():
    validate_source(_EVENT_SOURCE)


def test_valid_entity_source():
    validate_source(_ENTITY_SOURCE)


def test_valid_join_source():
    validate_source(_JOIN_SOURCE)


def test_valid_source_with_events():
    source = ttypes.Source(
        events=_EVENT_SOURCE
    )
    validate_source(source)


def test_invalid_source_with_events():
    source = ttypes.Source(
        events=_ENTITY_SOURCE
    )
    with pytest.raises(AssertionError) as e:
        validate_source(source)
    assert "Source.events must be of type EventSource" in str(e)


def test_valid_source_with_entities():
    source = ttypes.Source(
        entities=_ENTITY_SOURCE
    )
    validate_source(source)


def test_invalid_source_with_entities():
    source = ttypes.Source(
        entities=_JOIN_SOURCE
    )
    with pytest.raises(AssertionError) as e:
        validate_source(source)
    assert "Source.entities must be of type EntitySource" in str(e)


def test_valid_source_with_join_source():
    source = ttypes.Source(
        joinSource=_JOIN_SOURCE
    )
    validate_source(source)


def test_invalid_source_with_join_source():
    source = ttypes.Source(
        joinSource=_EVENT_SOURCE
    )
    with pytest.raises(AssertionError) as e:
        validate_source(source)
    assert "Source.joinSource must be of type JoinSource" in str(e)
