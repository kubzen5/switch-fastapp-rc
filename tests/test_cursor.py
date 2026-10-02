from app.domain.cursor import SyncCursor


def test_same_timestamp_orders_by_numeric_key_across_batch_boundary():
    boundary = SyncCursor(source_updated_at="2026-09-30T10:00:00Z", order_key=9)
    following = SyncCursor(source_updated_at="2026-09-30T12:00:00+02:00", order_key=10)
    assert following.follows(boundary)
    assert not boundary.follows(boundary)
    assert not boundary.follows(following)
