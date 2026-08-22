# Void: floors measured at 5 slices over a 6-shard index

These records were produced with `run_es.py`'s old hardcoded `SLICES = 5`, a leftover
from the era when the main index had 5 shards. Against `bench_events_10m` (6 primary
shards) Elasticsearch assigns one slice two shards; that slice does double the work and
the wall clock is the slowest slice, so the sliced floor is roughly 2/6 of the corpus
behind rather than 1/6.

They are kept because they are an honest measurement of a real (mis)configuration, and
because deleting them would leave the session's history unexplained. They are NOT a
floor and must never be quoted as one, or compared with any engine figure.

The replacement floors in the parent directory derive the slice count from the index's
actual `number_of_shards`.
