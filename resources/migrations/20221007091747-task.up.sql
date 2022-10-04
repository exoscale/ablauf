CREATE TABLE IF NOT EXISTS task (
    id         int         AUTO_INCREMENT,
    type       enum('workflow', 'action')        NOT NULL,
    status     enum('new', 'success', 'failure') NOT NULL DEFAULT 'new',
    wid        int                               NOT NULL,
    wuuid      varchar(36)                       NOT NULL,
    payload    text,
    process_at timestamp                         NOT NULL DEFAULT now(),
    FOREIGN KEY(wid)       REFERENCES workflow_run(id),
    PRIMARY KEY(id)
);
--;;
CREATE INDEX task_process ON task (process_at);
