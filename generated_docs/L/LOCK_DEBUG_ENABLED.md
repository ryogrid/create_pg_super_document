# LOCK_DEBUG_ENABLED

## Location
src/backend/storage/lmgr/lock.c: 305 - 315

## Overview
LOCK_DEBUG_ENABLED is an inline static function that determines whether lock debugging should be enabled for a specific lock based on its tag and global debugging configuration.

## Definition

```c
inline static bool
LOCK_DEBUG_ENABLED(const LOCKTAG *tag)
```
## Detailed Description
LOCK_DEBUG_ENABLED checks whether debugging should be enabled for a particular lock by examining the lock's tag and comparing it against configured debugging parameters. The function returns true if either of two conditions are met:

1. The lock method's trace_flag is enabled AND the lock's field2 (typically an OID) is greater than or equal to Trace_lock_oidmin
2. A specific table is being traced (Trace_lock_table is set) AND the lock's field2 matches the traced table OID

This selective debugging mechanism allows PostgreSQL developers to focus on specific objects or ranges of objects while avoiding noise from system catalogs and internal operations.

## Parameters / Member Variables
- : Pointer to a LOCKTAG structure containing the lock identifier information
  - : Index into LockMethods array to get the appropriate lock method
  - : Typically contains the OID of the object being locked

## Dependencies
- Functions called/Symbols referenced:
  - LOCKTAG (structure)
  - LockMethods (global array)
  - Trace_lock_oidmin (global variable)
  - Trace_lock_table (global variable)
- Called from (representative examples):
  - LockHasWaiters (src/backend/storage/lmgr/lock.c:661)
  - LockAcquireExtended (src/backend/storage/lmgr/lock.c:817)
  - LockRelease (src/backend/storage/lmgr/lock.c:1982)

## Notes and Other Information
- This function is only compiled when LOCK_DEBUG is defined at compile time
- The trace_flag is accessed through the LockMethods array using the lock method ID from the tag
- Trace_lock_oidmin defaults to FirstNormalObjectId to exclude system catalog debugging by default
- The function uses short-circuit evaluation - if the first condition is false, the second condition is still evaluated
- This is part of PostgreSQL's debugging infrastructure and has no impact on production builds unless LOCK_DEBUG is enabled