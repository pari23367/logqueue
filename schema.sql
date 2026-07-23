CREATE TABLE tasks (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    payload      JSONB NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending'
                 CHECK (status IN ('pending', 'in_progress', 'completed', 'dead_letter')),
    attempts     INT NOT NULL DEFAULT 0,
    run_after    TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_by    TEXT,
    locked_at    TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);

-- supports the Phase 3 claim query (oldest claimable pending task)
CREATE INDEX tasks_claimable_idx ON tasks (run_after) WHERE status = 'pending';

-- Evidence table: one row per claim a worker believes it made.
-- Used to prove double-execution independently of worker logs.
CREATE TABLE task_executions (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    task_id    BIGINT NOT NULL REFERENCES tasks(id),
    worker_id  TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX task_executions_task_id_idx ON task_executions (task_id);
