# Decisions

## Phase 1: schema

**Why `jsonb` for `payload` instead of `text`**
The payload is structured data (a log line plus, later, the parsed
result) that the app needs to read and update fields on — not just
store and return opaquely. `jsonb` stores it in a decomposed binary
format, so Postgres can index into it, and later query/update
sub-fields (e.g. `payload || '{"parsed": ...}'` in the worker) without
parsing a text blob in application code first. Plain `text` would work
as a dumb bag of bytes, but pushes all structure-awareness into the
app and loses the ability to ever query on payload contents in SQL.

**What `run_after` is for**
It's the earliest time a task is eligible to be claimed — the claim
query filters on `status = 'pending' AND run_after <= now()`. On
insert it defaults to `now()` (claimable immediately). Its real job
shows up in Phase 4: on a failed attempt, the retry sets
`run_after = now() + backoff(attempts)` instead of putting the task
back to `pending` immediately, so retries back off exponentially
instead of hammering the queue (and whatever downstream dependency
just failed) in a tight loop.

**What the partial index on the claim query does**
`CREATE INDEX tasks_claimable_idx ON tasks (run_after) WHERE status = 'pending';`
It's a partial index — it only indexes rows where `status = 'pending'`,
which is exactly the subset the claim query scans. Two benefits: the
index stays small and cheap to maintain (completed/dead-lettered rows,
which will eventually be the majority of the table, are never in it),
and the claim query's `ORDER BY run_after` for the claimable set can
be satisfied by an index scan instead of a sequential scan over the
whole table as it grows.

**Why `locked_by` and `locked_at` are separate columns**
They answer two different questions and get used by two different
consumers. `locked_at` is what the reaper cares about — it's a
timestamp, compared against "now minus lease duration" to decide a
lease has expired and the task should be reclaimed; that comparison
doesn't need to know *who* held it. `locked_by` is for
attribution/debugging — which worker instance had the task when it
went stale — and isn't used in the expiry comparison at all. Collapsing
them into one column (e.g. a single "locked by X at time Y" string)
would force parsing a composite value just to do the expiry check that
only ever needs the timestamp half.
