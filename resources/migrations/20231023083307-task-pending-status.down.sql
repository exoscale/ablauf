UPDATE task set status='failure' where status='pending'
--;;
ALTER TABLE task MODIFY COLUMN
status enum('new', 'success', 'failure')
NOT NULL DEFAULT 'new' AFTER `type`;
--;;
ALTER TABLE task DROP COLUMN updated_at;