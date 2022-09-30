DROP TABLE IF EXISTS task;
DROP TABLE if EXISTS workflow_run;

CREATE TABLE IF NOT EXISTS workflow_run (
    id         int           AUTO_INCREMENT,
    status     enum('pending', 'success', 'failure') NOT NULL DEFAULT 'pending',
    reason     varchar(255),
    ast        text          NOT NULL,
    context    text          NOT NULL,
    created_at timestamp     NOT NULL DEFAULT now(),
    updated_at timestamp     NOT NULL DEFAULT now() ON UPDATE NOW(),
    PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS task (
    id         int         AUTO_INCREMENT,
    type       enum('workflow', 'action') NOT NULL,
    wid        int,
    payload    text,
    process_at timestamp   NOT NULL DEFAULT now(),
    PRIMARY KEY(id),
    FOREIGN KEY(wid) REFERENCES workflow_run(id)
);
