"""Canonical business projection fingerprint, independent of delivery audit."""
import hashlib
import json


def state_fingerprint(connection):
    # Exclude last_captured_at/updated_at and all delivery/processing metadata.
    # Include every entity, its source version and source occurrence timestamp.
    rows = connection.execute('''SELECT source, entity_key, source_version,
        payload, total_price, last_event_id, source_updated_at FROM orders_current''').fetchall()
    records = []
    from app.domain.envelope import utc_timestamp
    for row in rows:
        records.append({**row, 'total_price': format(row['total_price'], '.2f'),
                        'source_updated_at': utc_timestamp(row['source_updated_at'])})
    records.sort(key=lambda row: (row['source'], row['entity_key']))
    encoded = json.dumps(records, sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode()
    return {'entities': len(records), 'sha256': hashlib.sha256(encoded).hexdigest()}
