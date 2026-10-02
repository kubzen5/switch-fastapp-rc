"""Run one observable review phase inside the isolated Compose adapter service."""
import json
import os
from pathlib import Path
import sys
import time
from contextlib import closing
from uuid import uuid4
from urllib.parse import urlencode
from urllib.request import urlopen

from confluent_kafka import Consumer, Producer, TopicPartition

from app.adapter import demo
from app.adapter.coordination import source_lock
from app.adapter.main import sync_once
from app.adapter.snowflake_client import connect_source, source_table
from app.adapter.snowflake_client import SnowflakeReader
from app.config import get_settings
from app.consumer.coordination import consumer_lock
from app.consumer.main import process_message
from app.db.connection import connect
from app.db.state import state_fingerprint
from app.logging import configure_logging
from app.runtime import wait_for_dependencies
from app.transport.producer import PublicationFailed


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def snapshot(settings):
    with connect(settings) as conn:
        conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        return {
            'state': state_fingerprint(conn),
            'logical_events': conn.execute('SELECT count(*) AS n FROM processed_events').fetchone()['n'],
            'deliveries': conn.execute('SELECT count(*) AS n FROM event_deliveries').fetchone()['n'],
            'rejections': conn.execute('SELECT count(*) AS n FROM event_rejections').fetchone()['n'],
            'source_quarantine': conn.execute('SELECT count(*) AS n FROM source_quarantine').fetchone()['n'],
            'checkpoint': conn.execute('SELECT source,last_updated_at,last_order_key,last_completed_batch_id FROM sync_state ORDER BY source').fetchall(),
        }


def api(path, **params):
    with urlopen('http://api:8000' + path + ('?' + urlencode(params) if params else ''), timeout=10) as response:
        return json.load(response)


def topic_ends(settings):
    client = Consumer({'bootstrap.servers': settings.kafka_bootstrap_servers,
                       'group.id': 'review-observer-' + uuid4().hex,
                       'enable.auto.commit': False})
    try:
        meta = client.list_topics(settings.kafka_topic, timeout=10)
        require(meta.topics[settings.kafka_topic].error is None, 'Topic metadata error')
        return {p: client.get_watermark_offsets(TopicPartition(settings.kafka_topic, p), timeout=10)
                for p in sorted(meta.topics[settings.kafka_topic].partitions)}
    finally:
        client.close()


def wait_snapshot(settings, logical, rejected=0):
    deadline = time.monotonic() + 300
    while True:
        result = snapshot(settings)
        if result['logical_events'] == logical and result['rejections'] == rejected:
            return result
        require(time.monotonic() < deadline, 'Consumer convergence timeout')
        time.sleep(1)


def write_source(settings, action, rows):
    with source_lock(settings) as coordinator:
        coordinator.execute('UPDATE source_write_fences SET blocked=true WHERE source=%s', (settings.source_name,))
        with closing(connect_source(settings, use_database=action != 'prepare')) as conn:
            if action == 'prepare':
                demo.prepare(conn, settings, rows)
            elif action == 'bad':
                table = source_table(settings)
                demo.execute(conn, 'BEGIN')
                stamp = demo.epoch(conn, settings)
                key = demo.execute(conn, f'SELECT MAX(order_key)+1 FROM {table}')[0][0]
                demo.execute(conn, f'''INSERT INTO {table}
                    SELECT %s,customer_key,customer_name,order_status,-1,order_date,1,%s,'insert'
                    FROM {table} ORDER BY order_key LIMIT 1''', (key, stamp))
                demo.execute(conn, f'INSERT INTO {table}_CHANGES SELECT * FROM {table} WHERE order_key=%s', (key,))
                demo.execute(conn, 'COMMIT')
            else:
                demo.mutate(conn, settings)
        coordinator.execute('UPDATE source_write_fences SET blocked=false WHERE source=%s', (settings.source_name,))


def compare_source(settings):
    """Full business payload + version equality, not just record counts."""
    from app.domain.envelope import OrderPayload
    with closing(connect_source(settings)) as snowflake:
        table = source_table(settings)
        source_rows = demo.execute(snowflake, f'''SELECT order_key,customer_key,customer_name,
            order_status,total_price,order_date,source_version FROM {table} WHERE total_price>=0 ORDER BY order_key''')
        source = {f'order:{r[0]}': {'source_version': r[6], 'payload': OrderPayload(**dict(zip(
            ('order_key','customer_key','customer_name','order_status','total_price','order_date'), r[:6]))).model_dump(mode='json')}
            for r in source_rows}
        collisions = demo.execute(snowflake, f'''SELECT COUNT(*) FROM (
            SELECT source_updated_at,order_key FROM {table}_CHANGES
            GROUP BY source_updated_at,order_key HAVING COUNT(*)>1)''')[0][0]
        require(collisions == 0, 'Duplicate source cursor')
    with connect(settings) as conn:
        target = {r['entity_key']: {'source_version': r['source_version'], 'payload': r['payload']}
                  for r in conn.execute('SELECT entity_key,source_version,payload FROM orders_current WHERE source=%s', (settings.source_name,)).fetchall()}
    require(source == target, 'Snowflake and PostgreSQL business projections differ')
    return {'all_payloads_and_versions_equal': True, 'entities_compared': len(source), 'cursor_collisions': collisions}


def verify_api(settings, expected):
    stats = api('/stats')
    require(stats['counts']['unique_events'] == expected, 'API logical count mismatch')
    require(stats['processing_delay']['sample_count'] == expected, 'API lag sample mismatch')
    page = api('/events', source=settings.source_name, limit=2)
    following = api('/events', source=settings.source_name, limit=2, after_id=page['next_after_id'])
    require(len(page['items']) == len(following['items']) == 2, 'API pagination failed')
    require(page['items'][-1]['delivery_id'] < following['items'][0]['delivery_id'], 'API pagination overlaps')
    key = page['items'][0]['entity_key']
    entity = api('/entities/' + key, source=settings.source_name)
    require(entity['current']['source_version'] == 3, 'Repeated source update lost')
    require({e['envelope']['source_version'] for e in entity['history']} == {1, 2, 3}, 'Entity history lost a version')
    updates = api('/events', source=settings.source_name, event_type='update', limit=500)
    require(len(updates['items']) == 4, 'API update filter mismatch')
    instant = page['items'][0]['occurred_at']
    bounded = api('/events', source=settings.source_name, entity_key=key, event_type='insert', since=instant, until=instant)
    require(len(bounded['items']) == 1, 'API inclusive time/entity/type filters failed')
    require(len(stats['watermarks']) == 1 and stats['watermarks'][0]['last_sync_completed_at'], 'Missing API watermark')
    return {'pagination': True, 'filters': True, 'entity_history_versions': [1, 2, 3], 'stats': stats}


def inject_invalid(settings):
    with connect(settings) as conn:
        envelope = conn.execute('SELECT envelope FROM processed_events ORDER BY source,entity_key,source_version LIMIT 1').fetchone()['envelope']
    key = envelope['entity_key'].encode()
    negative = json.loads(json.dumps(envelope))
    negative['payload']['total_price'] = '-1.00'
    schema = json.loads(json.dumps(envelope))
    del schema['source_version']
    conflict = json.loads(json.dumps(envelope))
    conflict['payload']['customer_name'] += ' conflict'
    producer = Producer({'bootstrap.servers': settings.kafka_bootstrap_servers, 'acks': 'all', 'delivery.timeout.ms': 10000})
    outcomes = []
    for raw in (b'{bad-json', json.dumps(negative).encode(), json.dumps(schema).encode(), json.dumps(conflict).encode()):
        producer.produce(settings.kafka_topic, key=key, value=raw, on_delivery=lambda err, msg: outcomes.append(err))
    require(producer.flush(15) == 0 and len(outcomes) == 4 and all(e is None for e in outcomes), 'Quarantine injection was not ACKed')


def replay(settings):
    before = snapshot(settings)
    group = 'review-replay-' + uuid4().hex
    client = Consumer({'bootstrap.servers': settings.kafka_bootstrap_servers, 'group.id': group,
                       'auto.offset.reset': 'earliest', 'enable.auto.commit': False,
                       'enable.auto.offset.store': False})
    try:
        with consumer_lock(settings), connect(settings) as conn:
            ends = topic_ends(settings)
            require(all(low == 0 for low, high in ends.values()), 'Offset 0 expired; cannot claim full replay')
            partitions = [TopicPartition(settings.kafka_topic, p, 0) for p in ends]
            require(all(p.offset < 0 for p in client.committed(partitions, timeout=10)), 'Replay group already has committed offsets')
            client.assign(partitions)  # Explicit offset 0 in EVERY partition.
            positions = {p: 0 for p in ends}
            consumed = 0
            deadline = time.monotonic() + 600
            while any(positions[p] < ends[p][1] for p in ends):
                require(time.monotonic() < deadline, 'Replay timeout')
                message = client.poll(1)
                if message is None:
                    continue
                require(message.error() is None, 'Replay broker error')
                require(message.offset() < ends[message.partition()][1], 'Concurrent topic write during replay')
                process_message(client, conn, message)
                positions[message.partition()] = message.offset() + 1
                consumed += 1
            committed = client.committed(partitions, timeout=10)
            require(all(p.offset == ends[p.partition][1] for p in committed), 'Replay offsets not durably committed')
        after = snapshot(settings)
        require(before['state'] == after['state'], 'Replay changed business state hash')
        require(before['logical_events'] == after['logical_events'], 'Replay created logical events')
        require(after['deliveries'] - before['deliveries'] == consumed, 'Replay delivery audit incomplete')
        require(topic_ends(settings) == ends, 'Replay changed broker end offsets')
        return {'group': group, 'start_offsets': {p: 0 for p in ends}, 'end_offsets': {p: h for p, (l, h) in ends.items()},
                'consumed': consumed, 'before': before, 'after': after}
    finally:
        client.close()


def run(phase):
    if phase == 'summary':
        phases = ('prepare','initial','cursor-collision','no-change','mutate','incremental',
                  'restart','exclusion','mutate-outage','outage','recovery','quarantine','replay','final')
        reports = {p: json.loads(Path('/evidence', p + '.json').read_text()) for p in phases}
        require(all(r['result'] == 'PASS' for r in reports.values()), 'At least one phase failed')
        return {'phases': {p: r['result'] for p, r in reports.items()},
                'replay': reports['replay']['evidence']}
    settings = get_settings()
    rows = int(os.environ.get('REVIEW_ROWS', '10000'))
    configure_logging('WARNING')
    if phase != 'outage':
        wait_for_dependencies(settings)
    if phase == 'prepare':
        require(10000 <= rows <= 50000, 'REVIEW_ROWS must be between 10000 and 50000')
        require(snapshot(settings)['logical_events'] == 0, 'Review requires a fresh database')
        write_source(settings, 'prepare', rows)
        return {'seed_rows': rows, 'source': settings.source_name, 'table': source_table(settings)}
    if phase in ('mutate', 'mutate-outage'):
        write_source(settings, 'mutate', rows)
        return {'new_versions': 5, 'inserts': 1, 'updates': 4}
    if phase == 'cursor-collision':
        # Session-local table: no corruption of the actual review journal.
        with closing(connect_source(settings)) as conn:
            test_settings = settings.model_copy(update={'snowflake_table': settings.snowflake_table + '_COLLISION'})
            table = source_table(test_settings) + '_CHANGES'
            demo.execute(conn, f'CREATE TEMPORARY TABLE {table} LIKE {source_table(settings)}_CHANGES')
            for _ in range(2):
                demo.execute(conn, f'INSERT INTO {table} SELECT * FROM {source_table(settings)}_CHANGES ORDER BY order_key LIMIT 1')
            reader = SnowflakeReader(conn, test_settings)
            try:
                reader.read_batch(None, reader.high_watermark(), 1)
            except RuntimeError as error:
                require('duplicate cursors' in str(error), 'Unexpected cursor error')
            else:
                raise AssertionError('Duplicate cursor at LIMIT boundary was silently lost')
        return {'real_snowflake_duplicate_at_limit_1_rejected': True}
    if phase in ('initial', 'incremental', 'recovery'):
        expected = rows + {'initial': 0, 'incremental': 5, 'recovery': 10}[phase]
        seen = sync_once(settings)
        require(seen == (rows if phase == 'initial' else 5), 'Unexpected sync record count')
        result = wait_snapshot(settings, expected)
        result['source_comparison'] = compare_source(settings)
        if phase == 'incremental':
            result['api'] = verify_api(settings, expected)
        return result
    if phase in ('no-change', 'restart', 'final'):
        expected = rows + {'no-change': 0, 'restart': 5, 'final': 10}[phase]
        before = wait_snapshot(settings, expected, rejected=8 if phase == 'final' else 0)
        ends = topic_ends(settings)
        require(sync_once(settings) == 0, 'Unchanged source emitted records')
        after = snapshot(settings)
        require(before == after, 'Unchanged source altered checkpoint or sink')
        require(ends == topic_ends(settings), 'Unchanged source emitted Kafka messages')
        return {'before': before, 'after': after, 'broker_offsets_unchanged': True}
    if phase == 'exclusion':
        deadline = time.monotonic() + 60
        while True:
            try:
                with consumer_lock(settings.model_copy(update={'kafka_group_id': 'different-demo-group'})):
                    pass
            except RuntimeError as error:
                require('Another demo consumer' in str(error), 'Unexpected lock error')
                break
            require(time.monotonic() < deadline, 'Normal consumer did not acquire demo lock')
            time.sleep(1)
        return {'second_consumer_rejected_across_group_ids': True}
    if phase == 'outage':
        before = snapshot(settings)
        try:
            sync_once(settings)
        except PublicationFailed:
            pass
        else:
            raise AssertionError('Broker outage unexpectedly delivered')
        require(snapshot(settings) == before, 'Outage advanced checkpoint or business state')
        return {'failed_publication': True, 'checkpoint_retained': True, 'before': before}
    if phase == 'quarantine':
        before = wait_snapshot(settings, rows + 10)
        write_source(settings, 'bad', rows)
        require(sync_once(settings) == 1, 'Bad source record not inspected')
        inject_invalid(settings)
        after = wait_snapshot(settings, rows + 10, rejected=4)
        require(after['source_quarantine'] == 1, 'Bad Snowflake row not durably quarantined')
        require(before['state'] == after['state'], 'Bad records changed business state')
        with connect(settings) as conn:
            reasons = {r['reason_code']: r['n'] for r in conn.execute('SELECT reason_code,count(*) AS n FROM event_rejections GROUP BY reason_code').fetchall()}
        require(set(reasons) == {'invalid_json','invalid_envelope','business_quality_violation','source_version_conflict'}, 'Missing quality check evidence')
        return {'before': before, 'after': after, 'reasons': reasons}
    if phase == 'replay':
        return replay(settings)
    raise ValueError('Unknown review phase')


if __name__ == '__main__':
    phase = sys.argv[1]
    try:
        result = {'phase': phase, 'result': 'PASS', 'evidence': run(phase)}
    except Exception as error:
        # Connector exceptions may contain secrets: never write their text.
        result = {'phase': phase, 'result': 'FAIL', 'error_type': type(error).__name__}
        if isinstance(error, AssertionError):
            result['assertion'] = str(error)
    output = json.dumps(result, default=str, sort_keys=True)
    Path('/evidence', phase + '.json').write_text(output + '\n')
    print(output, flush=True)
    sys.exit(0 if result['result'] == 'PASS' else 1)
