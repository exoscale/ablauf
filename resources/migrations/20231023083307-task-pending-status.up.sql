ALTER TABLE task
MODIFY COLUMN
status enum('new', 'success', 'failure', 'pending')
NOT NULL DEFAULT 'new' AFTER `type`;
--;;
ALTER TABLE task
ADD COLUMN
updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;
