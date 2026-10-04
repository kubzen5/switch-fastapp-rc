from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from app.adapter import simulate


@pytest.fixture
def writer(monkeypatch):
    coordinator, snowflake = Mock(), Mock()
    settings = SimpleNamespace(source_name='test.orders')

    @contextmanager
    def lock(settings):
        yield coordinator

    monkeypatch.setattr(simulate, 'source_lock', lock)
    monkeypatch.setattr(simulate, 'connect_source', Mock(return_value=snowflake))
    monkeypatch.setattr(simulate, 'insert_orders', Mock(return_value=(101, 105)))
    return settings, coordinator, snowflake


def test_success_clears_fence_only_after_source_connection_close(monkeypatch, writer):
    settings, coordinator, snowflake = writer
    coordinator.execute.side_effect = lambda sql, params: (
        snowflake.close.assert_called_once() if 'blocked=false' in sql else None
    )
    assert simulate.write_batch(settings, SimpleNamespace(is_set=lambda: False)) == (101, 105)
    assert 'blocked=true' in coordinator.execute.call_args_list[0].args[0]
    assert 'blocked=false' in coordinator.execute.call_args_list[1].args[0]


@pytest.mark.parametrize('failure', ['connect', 'write', 'close'])
def test_uncertain_batch_retains_fence(monkeypatch, writer, failure):
    settings, coordinator, snowflake = writer
    error = RuntimeError('uncertain outcome')
    if failure == 'connect':
        simulate.connect_source.side_effect = error
    elif failure == 'write':
        simulate.insert_orders.side_effect = error
    else:
        snowflake.close.side_effect = error
    with pytest.raises(RuntimeError, match='uncertain outcome'):
        simulate.write_batch(settings, SimpleNamespace(is_set=lambda: False))
    assert coordinator.execute.call_count == 1
    assert 'blocked=true' in coordinator.execute.call_args.args[0]


def test_stop_while_waiting_for_source_lock_does_not_start_write(writer):
    settings, coordinator, snowflake = writer
    assert simulate.write_batch(settings, SimpleNamespace(is_set=lambda: True)) is None
    coordinator.execute.assert_not_called()
    simulate.connect_source.assert_not_called()


def test_empty_source_refused_before_transaction(monkeypatch):
    monkeypatch.setattr(simulate, 'source_table', lambda _: 'DEMO.PUBLIC.ORDERS')
    execute = Mock(return_value=[(None, None)])
    monkeypatch.setattr(simulate, 'execute', execute)
    with pytest.raises(RuntimeError, match='existing prepared source'):
        simulate.insert_orders(object(), object())
    assert execute.call_count == 1


def test_source_and_journal_are_written_in_one_transaction(monkeypatch):
    statements = []
    stamp = datetime.now(timezone.utc)

    def execute(connection, sql, params=()):
        statements.append((sql, params))
        return [(1, 100)] if sql.startswith('SELECT MIN') else []

    monkeypatch.setattr(simulate, 'execute', execute)
    monkeypatch.setattr(simulate, 'source_table', lambda _: 'DEMO.PUBLIC.ORDERS')
    monkeypatch.setattr(simulate, 'epoch', lambda *_: stamp)
    assert simulate.insert_orders(object(), object()) == (101, 105)
    assert statements[1][0] == 'BEGIN'
    assert statements[2][1] == (101, stamp, 1)
    assert statements[3][1] == (101, 105)
    assert 'ORDERS_CHANGES' in statements[3][0]
    assert statements[4][0] == 'COMMIT'


def test_journal_failure_does_not_commit(monkeypatch):
    statements = []

    def execute(connection, sql, params=()):
        statements.append(sql)
        if sql.startswith('SELECT MIN'):
            return [(1, 100)]
        if 'INSERT INTO DEMO.PUBLIC.ORDERS_CHANGES' in sql:
            raise RuntimeError('journal unavailable')
        return []

    monkeypatch.setattr(simulate, 'execute', execute)
    monkeypatch.setattr(simulate, 'source_table', lambda _: 'DEMO.PUBLIC.ORDERS')
    monkeypatch.setattr(simulate, 'epoch', lambda *_: datetime.now(timezone.utc))
    with pytest.raises(RuntimeError, match='journal unavailable'):
        simulate.insert_orders(object(), object())
    assert 'COMMIT' not in statements


def test_bounded_run_includes_write_time_in_interval(monkeypatch, capsys):
    clock = iter([0, 1, 4, 5, 8, 9])
    monkeypatch.setattr(simulate.time, 'monotonic', lambda: next(clock))
    write = Mock(return_value=(101, 105))
    monkeypatch.setattr(simulate, 'write_batch', write)
    stopped = SimpleNamespace(is_set=lambda: False, wait=Mock())
    simulate.simulate(object(), stopped, interval=4, batches=3)
    assert write.call_count == 3
    assert [call.args[0] for call in stopped.wait.call_args_list] == [3, 3]
    assert '3 batches, 15 new orders' in capsys.readouterr().out


def test_slow_batch_does_not_schedule_a_catchup_burst(monkeypatch, capsys):
    clock = iter([0, 6, 6, 7])
    monkeypatch.setattr(simulate.time, 'monotonic', lambda: next(clock))
    monkeypatch.setattr(simulate, 'write_batch', Mock(return_value=(101, 105)))
    stopped = SimpleNamespace(is_set=lambda: False, wait=Mock())
    simulate.simulate(object(), stopped, interval=4, batches=2)
    stopped.wait.assert_called_once_with(0)
    assert 'exceeded the 4s target' in capsys.readouterr().out
