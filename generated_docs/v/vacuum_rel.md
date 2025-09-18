# vacuum_rel

## Location
src/backend/commands/vacuum.c: 1973 - 2318

## Overview
Vacuums a single heap relation by handling transaction management, locking, privilege checking, and dispatching to appropriate vacuum implementations (full or lazy), with support for TOAST table processing.

## Definition


## Detailed Description
This function orchestrates the vacuuming of a single relation through a complex process that ensures safety, proper locking, and comprehensive cleanup. The key aspects include:

1. **Transaction Management**: Starts a new transaction for the vacuum operation and manages transaction state throughout
2. **Process State Management**: Sets PROC_IN_VACUUM and PROC_VACUUM_FOR_WRAPAROUND flags for lazy vacuum to coordinate with other operations
3. **Snapshot Management**: Acquires snapshots to prevent pg_subtrans truncation and maintain proper visibility horizons
4. **Lock Management**: Acquires appropriate locks (ShareUpdateExclusiveLock for lazy vacuum, AccessExclusiveLock for full vacuum) and session-level locks
5. **Privilege and Validity Checking**: Validates permissions, relation types, and handles special cases like temp tables and partitioned tables
6. **Parameter Resolution**: Resolves vacuum options (index_cleanup, truncate) based on relation-specific settings when not explicitly specified
7. **Security Context Management**: Switches to table owner's userid for index function execution with restricted operations
8. **Vacuum Dispatch**: Calls either cluster_rel() for VACUUM FULL or table_relation_vacuum() for lazy vacuum
9. **TOAST Processing**: Recursively processes TOAST tables when requested while maintaining proper locking and privilege context

The function implements various safety checks and early exit conditions for unsupported relation types, privilege violations, or special cases like partitioned tables.

## Parameters / Member Variables
- : Object identifier of the relation to vacuum
- : RangeVar containing the relation name (used only for error reporting; may be stale)
- : VacuumParams structure containing vacuum options and configuration
- : Buffer access strategy to use during vacuum operations

## Dependencies
- Functions called/Symbols referenced:
  - [StartTransactionCommand](../S/StartTransactionCommand.md)
  - [CommitTransactionCommand](../C/CommitTransactionCommand.md)
  - PushActiveSnapshot
  - PopActiveSnapshot
  - GetTransactionSnapshot
  - vacuum_open_relation
  - vacuum_is_permitted_for_relation
  - [relation_close](../r/relation_close.md)
  - [LockRelationIdForSession](../L/LockRelationIdForSession.md)
  - [UnlockRelationIdForSession](../U/UnlockRelationIdForSession.md)
  - [GetUserIdAndSecContext](../G/GetUserIdAndSecContext.md)
  - [SetUserIdAndSecContext](../S/SetUserIdAndSecContext.md)
  - [NewGUCNestLevel](../N/NewGUCNestLevel.md)
  - [RestrictSearchPath](../R/RestrictSearchPath.md)
  - [AtEOXact_GUC](../A/AtEOXact_GUC.md)
  - [cluster_rel](../c/cluster_rel.md)
  - table_relation_vacuum
- Called from (representative examples):
  - vacuum (main vacuum entry point)
  - [vacuum_rel](vacuum_rel.md) (recursive call for TOAST tables)

## Notes and Other Information
- This is a static function, only accessible within the vacuum.c source file
- Returns true if it's safe to proceed with ANALYZE on the table, false otherwise
- The function is designed to work across multiple small transactions to avoid locking the entire database
- Implements a "one heap at a time" approach with overhead trade-offs for better concurrency
- Special handling for partitioned tables (skips actual vacuum work but allows ANALYZE)
- Handles both VACUUM FULL (via cluster_rel) and lazy vacuum (via table_relation_vacuum)
- TOAST table vacuum skips ANALYZE since statistics are not important for TOAST relations
- Uses injection points for testing when compiled with USE_INJECTION_POINTS
- Maintains session-level locks across the entire operation including TOAST processing
- The function must be called outside of any existing transaction context
- Parameter modification is done on a copy to avoid affecting TOAST table processing
- Security context is properly restored even if operations fail or are interrupted