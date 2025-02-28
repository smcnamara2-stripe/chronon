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

import ai.chronon.api.ttypes as ttypes

from typing import Union

ANY_SOURCE_TYPE = Union[
    ttypes.Source, ttypes.EventSource, ttypes.EntitySource, ttypes.JoinSource
]


def validate_source(source: ANY_SOURCE_TYPE):
    # There no validations for specific source types at the moment.
    if not isinstance(source, ttypes.Source):
        return
    if source.events is not None:
        assert isinstance(
            source.events, ttypes.EventSource), "Source.events must be of type EventSource. Use Source.entites for EntitySource and Source.joinSource for JoinSource"
    elif source.entities is not None:
        assert isinstance(
            source.entities, ttypes.EntitySource), "Source.entities must be of type EntitySource. Use Source.events for EventsSource and Source.joinSource for JoinSource"
    elif source.joinSource is not None:
        assert isinstance(
            source.joinSource, ttypes.JoinSource), "Source.joinSource must be of type JoinSource. Use Source.events for EventSource and Source.entites for EntitySource"
