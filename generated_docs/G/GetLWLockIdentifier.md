# GetLWLockIdentifier

## Location
[src/backend/storage/lmgr/lwlock.c:769-785](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lwlock.c#L769-L785)

## Overview
Returns an identifier string for an LWLock based on wait class and event information, serving as a bridge between the wait event system and LWLock tranche names.

## Definition
```c
const char *GetLWLockIdentifier(uint32 classId, uint16 eventId)
```

## Detailed Description
GetLWLockIdentifier is a public function that provides a standardized way to obtain human-readable identifiers for LWLocks within the PostgreSQL wait event reporting system. The function:

1. **Validates the wait class**: Uses an assertion to ensure the classId parameter matches PG_WAIT_LWLOCK, confirming this function is being called for LWLock events specifically.
2. **Maps event ID to tranche name**: Treats the eventId parameter as a tranche number and delegates to GetLWTrancheName to retrieve the appropriate name.

This function serves as an interface layer between PostgreSQL's wait event monitoring infrastructure and the LWLock subsystem, enabling consistent naming and identification of LWLocks in system monitoring, logging, and diagnostic tools.

## Parameters / Member Variables
- `classId`: The wait event class identifier, must be PG_WAIT_LWLOCK for LWLock events
- `eventId`: The specific event identifier, interpreted as an LWLock tranche number

## Dependencies
- Functions called/Symbols referenced:
  - PG_WAIT_LWLOCK (constant for LWLock wait event class)
  - [GetLWTrancheName](GetLWTrancheName.md) (function to retrieve tranche name from tranche ID)
  - Assert (assertion macro for parameter validation)

- Called from (representative examples):
  - [pgstat_get_wait_event](../p/pgstat_get_wait_event.md) (wait event statistics collection)
  - [LWLockMode](../L/LWLockMode.md) (related to LWLock mode operations in header definitions)

## Notes and Other Information
- This function is part of the public API (non-static) and can be called from other modules
- The function assumes that event IDs for LWLocks directly correspond to tranche numbers
- Parameter validation is performed via assertion, meaning invalid parameters will cause process termination in debug builds
- The function provides a clean abstraction layer, allowing the wait event system to obtain LWLock names without directly accessing LWLock internal data structures
- Return value is a const char pointer, indicating the caller should not modify the returned string

## Simplified Source

```c
const char *
GetLWLockIdentifier(uint32 classId, uint16 eventId)
{
    Assert(classId == PG_WAIT_LWLOCK);

    // Event IDs are tranche numbers for LWLocks
    return GetLWTrancheName(eventId);
}
```