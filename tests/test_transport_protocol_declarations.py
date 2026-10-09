"""Keep transport adapters explicit about the shared contracts they implement."""

from __future__ import annotations

from inspect import signature

import pytest

import execution_plane_transport_direct.ep_queries as direct_ep_queries
import execution_plane_transport_journal.ep_queries as journal_ep_queries
from execution_plane.commands.clusters import ClusterCommandHandler
from execution_plane.commands.control import CancelCommandHandler
from execution_plane.commands.runtime import EPExchangeObserver
from execution_plane.commands.targets import TargetCommandHandler
from execution_plane.commands.work import WorkCommandHandler
from execution_plane_client.api import CommandRuntime
from execution_plane_client.crypto import AESGCMCipher, PayloadCipher
from execution_plane_client.handlers import (
    CommandDispatcher,
    CompletionRecorder,
    ObservationHandler,
    TransactionalHandler,
)
from execution_plane_client.observers import ExchangeObserver, NullObserver
from execution_plane_client.queries import (
    CommandCancellationProvider,
    CommandQueryProvider,
    CommandStateProvider,
    SessionCommandQueryProvider,
)
from execution_plane_client.resources import ResourceIntentStore
from execution_plane_client.transport import TransactionalDispatcher, Transport
from execution_plane_transport_direct.completion_recorder import DirectCompletionRecorder
from execution_plane_transport_direct.ep_queries import EPDirectCommandQueryProvider
from execution_plane_transport_direct.ep_runtime import build_ep_direct_runtime
from execution_plane_transport_direct.queries import InMemoryCommandQueryProvider
from execution_plane_transport_direct.resource_intents import InMemoryResourceIntentStore
from execution_plane_transport_direct.service import DirectCommandRuntime
from execution_plane_transport_direct.transport import HttpCommandTransport
from execution_plane_transport_journal.completion_recorder import JournalCompletionRecorder
from execution_plane_transport_journal.ep_runtime import build_ep_journal_runtime
from execution_plane_transport_journal.intents import JournalIntents
from execution_plane_transport_journal.queries import JournalSessionCommandQueryProvider
from execution_plane_transport_journal.resource_intents import JournalResourceIntentStore
from execution_plane_transport_journal.service import JournalCommandRuntime
from execution_plane_transport_journal.transport import JournalTransport
from execution_plane_transport_journal.workers.handling import _SessionJournalDispatcher


@pytest.mark.parametrize(
    ("implementation", "protocol"),
    [
        (DirectCommandRuntime, CommandRuntime),
        (JournalCommandRuntime, CommandRuntime),
        (InMemoryCommandQueryProvider, CommandStateProvider),
        (EPDirectCommandQueryProvider, CommandStateProvider),
        (JournalSessionCommandQueryProvider, SessionCommandQueryProvider),
        (HttpCommandTransport, Transport),
        (HttpCommandTransport, CommandDispatcher),
        (HttpCommandTransport, TransactionalDispatcher),
        (JournalTransport, Transport),
        (JournalTransport, CommandDispatcher),
        (JournalTransport, TransactionalDispatcher),
        (InMemoryResourceIntentStore, ResourceIntentStore),
        (JournalResourceIntentStore, ResourceIntentStore),
        (JournalIntents, CommandCancellationProvider),
        (DirectCompletionRecorder, CompletionRecorder),
        (JournalCompletionRecorder, CompletionRecorder),
        (_SessionJournalDispatcher, CommandDispatcher),
        (EPExchangeObserver, ExchangeObserver),
        (NullObserver, ExchangeObserver),
        (AESGCMCipher, PayloadCipher),
        (WorkCommandHandler, TransactionalHandler),
        (ClusterCommandHandler, TransactionalHandler),
        (ClusterCommandHandler, ObservationHandler),
        (TargetCommandHandler, TransactionalHandler),
        (TargetCommandHandler, ObservationHandler),
        (CancelCommandHandler, TransactionalHandler),
        (CancelCommandHandler, ObservationHandler),
    ],
)
def test_implementation_declares_its_shared_protocol_base(implementation, protocol):
    assert protocol in implementation.__bases__


@pytest.mark.parametrize(
    ("module", "class_name", "protocol"),
    [
        (direct_ep_queries, "EPDirectCommandQueryProvider", CommandStateProvider),
        (journal_ep_queries, "EPJournalCommandQueryProvider", CommandQueryProvider),
    ],
)
def test_ep_query_adapters_use_parallel_names_and_declare_their_protocol(module, class_name, protocol):
    implementation = getattr(module, class_name, None)
    assert implementation is not None
    assert protocol in implementation.__bases__


@pytest.mark.parametrize(
    ("builder", "runtime"),
    [
        (build_ep_direct_runtime, DirectCommandRuntime),
        (build_ep_journal_runtime, JournalCommandRuntime),
    ],
)
def test_ep_runtime_builder_advertises_its_concrete_transport_runtime(builder, runtime):
    assert signature(builder).return_annotation == runtime.__name__
