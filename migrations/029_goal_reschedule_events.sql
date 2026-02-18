CREATE TABLE IF NOT EXISTS goal_reschedule_events (
    id INTEGER PRIMARY KEY,
    goal_id INTEGER NOT NULL REFERENCES goals(id),
    old_end_date TEXT NOT NULL,
    new_end_date TEXT NOT NULL,
    changed_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS ix_goal_reschedule_events_goal_id ON goal_reschedule_events(goal_id);
CREATE INDEX IF NOT EXISTS ix_goal_reschedule_events_changed_at ON goal_reschedule_events(changed_at);
