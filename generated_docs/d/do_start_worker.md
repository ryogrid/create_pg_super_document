# do_start_worker

## Location
[src/backend/postmaster/autovacuum.c:1073-1284](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/autovacuum.c#L1073-L1284)

## Overview
A bare-bones procedure for starting an autovacuum worker from the launcher that determines what database to work on, sets up shared memory structures, and signals the postmaster to start the worker.

## Definition


## Detailed Description
The  function implements the core logic for launching autovacuum workers from the autovacuum launcher. It performs intelligent database selection by prioritizing databases that are at risk of transaction ID wraparound or MultiXact ID wraparound, followed by databases that haven't been auto-vacuumed recently.

The function first checks if any worker slots are available in the shared memory worker pool. If no workers are free, it returns immediately. Otherwise, it creates a temporary memory context and retrieves a list of all databases in the cluster.

The database selection algorithm prioritizes databases based on urgency:
1. Databases at risk of XID wraparound (datfrozenxid older than autovacuum_freeze_max_age)
2. Databases at risk of MultiXact wraparound (datminmxid older than MultiXactMemberFreezeThreshold)
3. Databases with the oldest last_autovac_time from pgstat

The function also implements throttling by skipping databases that were recently processed (within autovacuum_naptime seconds) according to the internal DatabaseList tracking.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - / (AutovacuumLock coordination)
  -  (check for available workers)
  - / (memory management)
  -  (retrieve database information)
  - / (get current transaction IDs)
  - / (wraparound risk detection)
  -  (database statistics)
  -  (time-based throttling)
  -  (PMSIGNAL_START_AUTOVAC_WORKER)
  -  (database list maintenance)

- Called from (representative examples):
  -  (src/backend/postmaster/autovacuum.c:1290)

## Notes and Other Information
- Returns the OID of the database that will be processed, or InvalidOid if no worker was started
- Fails gracefully when autovacuum_workers are already at capacity
- Uses sophisticated prioritization to prevent transaction ID wraparound disasters
- Implements temporal throttling to avoid repeatedly selecting the same database
- Creates temporary memory context to prevent memory leaks during database list processing
- Critical for maintaining database health in PostgreSQL clusters with multiple databases