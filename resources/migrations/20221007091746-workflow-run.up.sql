CREATE TABLE IF NOT EXISTS workflow_run (
    id         int           AUTO_INCREMENT,
    uuid       varchar(36)   NOT NULL,
    status     enum('pending', 'success', 'failure') NOT NULL DEFAULT 'pending',
    job_owner  varchar(64)   NOT NULL,
    job_system varchar(64)   NOT NULL,
    job_type   varchar(64)   NOT NULL,
    reason     varchar(255),
    ast        text          NOT NULL,
    metadata   text          NOT NULL,
    context    text          NOT NULL,
    created_at timestamp     NOT NULL DEFAULT now(),
    updated_at timestamp     NOT NULL DEFAULT now() ON UPDATE NOW(),
    PRIMARY KEY(id)
);
--;;
CREATE INDEX workflow_run_uuid_idx     ON workflow_run (uuid);
--;;
CREATE INDEX workflow_run_status_idx   ON workflow_run (status);
--;;
CREATE INDEX workflow_run_owner_idx    ON workflow_run (job_owner);
--;;
CREATE INDEX workflow_run_system_idx   ON workflow_run (job_system);
--;;
CREATE INDEX workflow_run_type_idx     ON workflow_run (job_type);
--;;
CREATE INDEX workflow_run_creation_idx ON workflow_run (created_at);
