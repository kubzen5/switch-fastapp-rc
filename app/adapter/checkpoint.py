import json
from uuid import uuid4
from psycopg.types.json import Jsonb
from psycopg.pq import TransactionStatus
from app.domain.cursor import SyncCursor


class PostgresCheckpoint:
    def __init__(self, connection):
        self.conn = connection

    def load(self, source):
        with self.conn.transaction():
            row = self.conn.execute('SELECT last_updated_at, last_order_key FROM sync_state WHERE source=%s', (source,)).fetchone()
            return SyncCursor(source_updated_at=row['last_updated_at'], order_key=row['last_order_key']) if row and row['last_updated_at'] else None

    def start_run(self, source, after, high):
        run_id = uuid4()
        with self.conn.transaction():
            # Any prior running attempt lost its worker; its checkpoint remains valid.
            self.conn.execute("UPDATE sync_batches SET status='failed', error_code='worker_restart', finished_at=now() WHERE status='running' AND run_id IN (SELECT run_id FROM sync_runs WHERE source=%s)", (source,))
            self.conn.execute("UPDATE sync_runs SET status='failed', finished_at=now() WHERE source=%s AND status='running'", (source,))
            self.conn.execute("INSERT INTO sync_runs(run_id,source,mode,status,high_watermark_at,high_watermark_order_key) VALUES (%s,%s,%s,'running',%s,%s)", (run_id, source, 'incremental' if after else 'initial', high.source_updated_at, high.order_key))
        return run_id

    def start_batch(self, run_id, sequence, after, end, count):
        batch_id = uuid4()
        with self.conn.transaction():
            self.conn.execute("INSERT INTO sync_batches(batch_id,run_id,batch_sequence,status,from_updated_at,from_order_key,to_updated_at,to_order_key,records_seen) VALUES (%s,%s,%s,'running',%s,%s,%s,%s,%s)", (batch_id, run_id, sequence, after.source_updated_at if after else None, after.order_key if after else None, end.source_updated_at, end.order_key, count))
        return batch_id

    def commit_batch(self, source, batch_id, end, events, rejected):
        if self.conn.info.transaction_status != TransactionStatus.IDLE:
            raise RuntimeError('commit_batch requires an idle connection to own the commit')
        with self.conn.transaction():
            for row, reason in rejected:
                self.conn.execute('INSERT INTO source_quarantine(batch_id,source,source_updated_at,order_key,raw_record,reason) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING', (batch_id, source, row['source_updated_at'], row['order_key'], Jsonb(json.loads(json.dumps(row, default=str))), reason))
            for event in events:
                self.conn.execute('''INSERT INTO source_entity_versions(source,entity_key,source_version,source_updated_at,last_event_id) VALUES (%s,%s,%s,%s,%s)
                    ON CONFLICT (source,entity_key) DO UPDATE SET source_version=excluded.source_version, source_updated_at=excluded.source_updated_at,last_event_id=excluded.last_event_id
                    WHERE excluded.source_version > source_entity_versions.source_version''', (source, event.entity_key, event.source_version, event.source_updated_at, event.event_id))
            self.conn.execute("UPDATE sync_batches SET status='published',records_published=%s,finished_at=now() WHERE batch_id=%s", (len(events), batch_id))
            self.conn.execute('''INSERT INTO sync_state(source,last_updated_at,last_order_key,last_completed_batch_id) VALUES (%s,%s,%s,%s)
                ON CONFLICT(source) DO UPDATE SET last_updated_at=excluded.last_updated_at,last_order_key=excluded.last_order_key,last_completed_batch_id=excluded.last_completed_batch_id,updated_at=now()''', (source, end.source_updated_at, end.order_key, batch_id))

    def finish_run(self, run_id, status):
        with self.conn.transaction():
            self.conn.execute('UPDATE sync_runs SET status=%s,finished_at=now() WHERE run_id=%s', (status, run_id))
            if status == 'failed':
                self.conn.execute("UPDATE sync_batches SET status='failed',error_code='sync_failure',finished_at=now() WHERE run_id=%s AND status='running'", (run_id,))
