from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from app.adapter import demo


@pytest.mark.parametrize('current_rows,journal_rows', [(20001, 20005), (0, 20000), (20000, 0)])
def test_prepare_refuses_existing_data_before_transaction(monkeypatch, current_rows, journal_rows):
    settings = SimpleNamespace(snowflake_database='demo', snowflake_schema='public', snowflake_table='orders')
    statements = []

    def execute(conn, sql, params=()):
        statements.append(sql)
        if sql.startswith('SELECT COUNT(*)'):
            return [(journal_rows if sql.endswith('_CHANGES') else current_rows,)]
        return []

    monkeypatch.setattr(demo, 'execute', execute)
    with pytest.raises(demo.PrepareRefused, match=f'has {current_rows} rows') as caught:
        demo.prepare(object(), settings, 20000)
    assert f'has {journal_rows} rows' in str(caught.value)
    assert not any(sql == 'BEGIN' or sql.startswith('INSERT') for sql in statements)


@pytest.fixture
def writer(monkeypatch):
    coordinator, snowflake = Mock(), Mock()
    settings = SimpleNamespace(source_name='test.orders')

    @contextmanager
    def source_lock(settings):
        yield coordinator

    monkeypatch.setattr('sys.argv', ['demo', 'prepare'])
    monkeypatch.setattr(demo, 'get_settings', lambda: settings)
    monkeypatch.setattr(demo, 'source_lock', source_lock)
    monkeypatch.setattr(demo, 'connect_source', lambda *args, **kwargs: snowflake)
    return coordinator, snowflake


def test_prepare_refusal_clears_its_fence_after_connection_close(monkeypatch, writer, capsys):
    coordinator, snowflake = writer
    monkeypatch.setattr(demo, 'prepare', Mock(side_effect=demo.PrepareRefused('Existing source data')))
    coordinator.execute.side_effect = lambda sql, params: (
        snowflake.close.assert_called_once() if 'blocked=false' in sql else None
    )
    with pytest.raises(SystemExit) as caught:
        demo.main()
    assert caught.value.code == 1
    assert 'Existing source data' in capsys.readouterr().err
    assert coordinator.execute.call_count == 2
    assert 'blocked=false' in coordinator.execute.call_args.args[0]


@pytest.mark.parametrize('failure', ['write', 'close', 'connect'])
def test_ambiguous_prepare_failure_retains_fence(monkeypatch, writer, failure):
    coordinator, snowflake = writer
    prepare = Mock()
    monkeypatch.setattr(demo, 'prepare', prepare)
    error = RuntimeError('ambiguous failure')
    if failure == 'write':
        prepare.side_effect = error
    elif failure == 'close':
        prepare.side_effect = demo.PrepareRefused('Existing source data')
        snowflake.close.side_effect = error
    else:
        monkeypatch.setattr(demo, 'connect_source', Mock(side_effect=error))
    with pytest.raises(RuntimeError, match='ambiguous failure'):
        demo.main()
    assert coordinator.execute.call_count == 1
    assert 'blocked=true' in coordinator.execute.call_args.args[0]


def test_prepare_does_not_clear_fence_from_an_earlier_writer(monkeypatch, writer):
    coordinator, snowflake = writer

    @contextmanager
    def source_lock(settings):
        raise RuntimeError('Source fenced')
        yield

    monkeypatch.setattr(demo, 'source_lock', source_lock)
    with pytest.raises(RuntimeError, match='Source fenced'):
        demo.main()
    coordinator.execute.assert_not_called()
    snowflake.close.assert_not_called()


@pytest.mark.parametrize('rows,expected', [([], 10000), (['--rows', '1'], 1), (['--rows', '50000'], 50000)])
def test_append_cli_reports_actual_rows_and_clears_fence_after_close(monkeypatch, writer, capsys, rows, expected):
    coordinator, snowflake = writer
    monkeypatch.setattr('sys.argv', ['demo', 'append', *rows])
    append = Mock(return_value=(1, 20001))
    monkeypatch.setattr(demo, 'append_orders', append)
    coordinator.execute.side_effect = lambda sql, params: (
        snowflake.close.assert_called_once() if 'blocked=false' in sql else None
    )
    demo.main()
    assert append.call_args.args[2] == expected
    assert coordinator.execute.call_count == 2
    assert f'requested={expected} added=1 total_orders=20001' in capsys.readouterr().out


@pytest.mark.parametrize('rows', ['0', '-1', '50001', 'invalid'])
def test_append_cli_rejects_invalid_count_before_connecting(monkeypatch, writer, rows):
    coordinator, snowflake = writer
    monkeypatch.setattr('sys.argv', ['demo', 'append', '--rows', rows])
    with pytest.raises(SystemExit) as caught:
        demo.main()
    assert caught.value.code == 2
    coordinator.execute.assert_not_called()
    snowflake.close.assert_not_called()


def test_append_refusal_clears_its_fence_after_close(monkeypatch, writer, capsys):
    coordinator, snowflake = writer
    monkeypatch.setattr('sys.argv', ['demo', 'append'])
    monkeypatch.setattr(demo, 'append_orders', Mock(side_effect=demo.AppendRefused('Run prepare first')))
    coordinator.execute.side_effect = lambda sql, params: (
        snowflake.close.assert_called_once() if 'blocked=false' in sql else None
    )
    with pytest.raises(SystemExit) as caught:
        demo.main()
    assert caught.value.code == 1
    assert 'Source fence cleared after append refusal' in capsys.readouterr().err
    assert coordinator.execute.call_count == 2


@pytest.mark.parametrize('failure', ['connect', 'write', 'close'])
def test_uncertain_append_retains_fence(monkeypatch, writer, failure):
    coordinator, snowflake = writer
    monkeypatch.setattr('sys.argv', ['demo', 'append'])
    append = Mock(return_value=(10, 20010))
    monkeypatch.setattr(demo, 'append_orders', append)
    error = RuntimeError('uncertain outcome')
    if failure == 'connect':
        monkeypatch.setattr(demo, 'connect_source', Mock(side_effect=error))
    elif failure == 'write':
        append.side_effect = error
    else:
        snowflake.close.side_effect = error
    with pytest.raises(RuntimeError, match='uncertain outcome'):
        demo.main()
    assert coordinator.execute.call_count == 1
    assert 'blocked=true' in coordinator.execute.call_args.args[0]
