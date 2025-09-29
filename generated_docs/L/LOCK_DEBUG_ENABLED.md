# LOCK_DEBUG_ENABLED

## Location
[src/backend/storage/lmgr/lock.c:305-315](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L305-L315)

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
  - [LOCKTAG](LOCKTAG.md) (structure)
  - LockMethods (global array)
  - Trace_lock_oidmin (global variable)
  - Trace_lock_table (global variable)
- Called from (representative examples):
  - [LockHasWaiters](LockHasWaiters.md) (src/backend/storage/lmgr/lock.c:661)
  - [LockAcquireExtended](LockAcquireExtended.md) (src/backend/storage/lmgr/lock.c:817)
  - [LockRelease](LockRelease.md) (src/backend/storage/lmgr/lock.c:1982)

## Notes and Other Information
- This function is only compiled when LOCK_DEBUG is defined at compile time
- The trace_flag is accessed through the LockMethods array using the lock method ID from the tag
- Trace_lock_oidmin defaults to FirstNormalObjectId to exclude system catalog debugging by default
- The function uses short-circuit evaluation - if the first condition is false, the second condition is still evaluated
- This is part of PostgreSQL's debugging infrastructure and has no impact on production builds unless LOCK_DEBUG is enabled

## Simplified Source

```c
// Simplified version of LOCK_DEBUG_ENABLED
inline static bool
LOCK_DEBUG_ENABLED(const LOCKTAG *tag)
{
    // Check if general tracing is enabled for this lock method and OID range
    bool general_trace = (LockMethods[tag->locktag_lockmethodid]->trace_flag &&
                         tag->locktag_field2 >= Trace_lock_oidmin);

    // Check if specific table tracing is enabled for this exact table
    bool specific_trace = (Trace_lock_table &&
                          tag->locktag_field2 == Trace_lock_table);

    // Enable debugging if either condition is met
    return general_trace || specific_trace;
}
```

Key simplifications made:
- Split the complex boolean expression into two clear intermediate variables
- Added descriptive comments explaining each tracing condition
- Maintained the original logic flow and return behavior
- Preserved the inline static function signature
- Made the OR logic more explicit and readable