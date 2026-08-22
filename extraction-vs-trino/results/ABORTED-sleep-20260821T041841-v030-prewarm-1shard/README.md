# Aborted: the HOST SLEPT mid-block (2026-08-21 ~08:10-09:30 local)

The flight leg (5 runs) and the splits probe completed. The Trino leg never
produced a record: macOS put the host to sleep, freezing the client while the
query kept running in the Docker VM -- Trino then killed both attempts with
ABANDONED_QUERY / ABANDONED_TASK ("results not accessed for 16 minutes"), and
the phase-3 log is silent from 08:02 to 09:31 for what is a ~25-minute phase.
S3 never ran. Nothing here is used by any table; the phase was re-run under
`caffeinate` as ...-1shard (fresh session).
