-- Retain policy, leases and compatibility view on rollback. Removing the shared
-- authority while older processes are draining would permit global overselling.
SELECT 1;
