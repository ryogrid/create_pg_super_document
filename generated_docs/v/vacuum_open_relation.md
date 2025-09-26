# vacuum_open_relation

## Location
src/backend/commands/vacuum.c: 769 - 880

## Overview
Attempts to open and lock a relation for vacuum or analyze operations, providing comprehensive error handling and logging when the relation cannot be accessed.

## Definition

```c
Relation
vacuum_open_relation(Oid relid, RangeVar *relation, bits32 options,
					 bool verbose, LOCKMODE lmode)
```
## Detailed Description
This function serves as a robust wrapper for opening relations that need to be vacuumed or analyzed. It handles the complexities of relation locking, including optional non-blocking lock acquisition and comprehensive error reporting when relations cannot be accessed.

The function implements sophisticated locking logic based on the VACOPT_SKIP_LOCKED option:
- Normal mode: Uses try_relation_open() directly with the requested lock mode
- Skip-locked mode: First attempts conditional locking, then opens with NoLock if successful

When relation access fails, the function provides detailed diagnostic information through appropriate log levels (WARNING for manual operations, LOG for autovacuum when verbose). It distinguishes between two failure scenarios: lock unavailability and relation non-existence, providing specific error codes and messages for each case.

The function handles race conditions gracefully - relations may disappear between discovery and processing, which is treated as a normal condition rather than an error.

## Parameters / Member Variables
- : Object identifier (OID) of the relation to open
- : RangeVar structure containing relation name information for error reporting (may be NULL)
- : Bitfield specifying the operation type (VACOPT_VACUUM, VACOPT_ANALYZE) and behavioral flags like VACOPT_SKIP_LOCKED
- : Boolean flag indicating whether verbose logging should be performed
- : Lock mode to acquire on the relation (e.g., ShareUpdateExclusiveLock for vacuum)

## Dependencies
- Functions called/Symbols referenced:
  - try_relation_open (safe relation opening)
  - ConditionalLockRelationOid (non-blocking lock acquisition)
  - AmAutoVacuumWorkerProcess (process type detection)
  - ereport (error/warning reporting)
  - ERRCODE_LOCK_NOT_AVAILABLE, ERRCODE_UNDEFINED_TABLE (error codes)
- Called from (representative examples):
  - vacuum_rel (main vacuum processing)
  - analyze_rel (analysis processing)

## Notes and Other Information
- Returns opened Relation on success, NULL on failure
- Implements different logging strategies for manual vs. autovacuum operations (WARNING vs. LOG level)
- For VACUUM ANALYZE operations, only logs VACUUM-related messages to avoid duplicate reporting
- Handles the SKIP_LOCKED option by attempting conditional locking before relation opening
- Gracefully handles relations that disappear during processing (common in high-concurrency environments)
- When RangeVar is NULL, skips detailed error reporting (allows callers to suppress logging intentionally)
- Error messages include specific relation names and distinguish between lock contention and missing relations