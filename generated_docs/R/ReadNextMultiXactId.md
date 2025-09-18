# ReadNextMultiXactId

## Location
src/backend/access/transam/multixact.c: 770 - 789

## Overview
ReadNextMultiXactId returns the next MultiXactId that would be assigned without actually allocating or consuming it.

## Definition
MultiXactId ReadNextMultiXactId(void)

## Detailed Description
This function provides read-only access to the next MultiXactId that will be assigned by the system. It does not advance the counter or allocate the ID - it simply returns what the next ID would be if requested. This is useful for various administrative and maintenance operations that need to know the current state of MultiXactId generation without affecting it.

The function implements a simple read operation protected by a shared lock on MultiXactGenLock. It reads the nextMXact value from the global MultiXactState and applies wraparound handling to ensure the returned value is at least FirstMultiXactId.

Key characteristics:
- Non-destructive read operation - does not advance the counter
- Thread-safe through shared locking
- Handles MultiXactId wraparound by enforcing minimum value
- Commonly used by vacuum operations, administrative functions, and monitoring tools

## Parameters / Member Variables
- No parameters (reads global state)

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire (with LW_SHARED)
  - LWLockRelease
- Global variables accessed:
  - MultiXactState->nextMXact
  - FirstMultiXactId
- Called from (representative examples):
  - refresh_by_heap_swap (src/backend/commands/matview.c:891)
  - ATRewriteTables (src/backend/commands/tablecmds.c:5868)
  - vacuum_get_cutoffs (src/backend/commands/vacuum.c:1129)
  - vacuum_xid_failsafe_check (src/backend/commands/vacuum.c:1286)
  - vac_update_relstats (src/backend/commands/vacuum.c:1531)
  - vac_update_datfrozenxid (src/backend/commands/vacuum.c:1630)
  - do_start_worker (src/backend/postmaster/autovacuum.c:1122)
  - AutoVacWorkerMain (src/backend/postmaster/autovacuum.c:1572)
  - mxid_age (src/backend/utils/adt/xid.c:123)

## Notes and Other Information
- Uses shared lock - allows concurrent reads while preventing writes during the read
- Comment suggests this operation could potentially be done without locking, indicating it's a simple atomic read
- Essential for vacuum operations to determine MultiXactId horizons and aging calculations
- Used by administrative tools and monitoring systems to track MultiXactId consumption
- Handles wraparound state by ensuring returned value is at least FirstMultiXactId
- Safe for concurrent access - multiple processes can call this simultaneously
- Does not modify any global state or advance counters