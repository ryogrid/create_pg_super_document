# WaitEventCustomNew

## Location
[src/backend/utils/activity/wait_event.c:176-276](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/wait_event.c#L176-L276)

## Overview
Core function that creates new custom wait events with proper concurrency control, name validation, and duplicate detection.

## Definition

```c
static uint32
WaitEventCustomNew(uint32 classId, const char *wait_event_name)
```
## Detailed Description
This internal function implements the core logic for creating custom wait events in PostgreSQL. It provides a robust, thread-safe mechanism for registering new wait events while ensuring name uniqueness within each wait event class and preventing resource exhaustion.

The function performs several key operations:

1. **Name validation**: Checks that the wait event name doesn't exceed NAMEDATALEN characters
2. **Duplicate detection**: Uses shared lock to check if the event name already exists
3. **Class consistency**: Ensures that if an event name exists, it belongs to the same class
4. **Atomic allocation**: Uses exclusive locking and spinlocks to safely allocate new event IDs
5. **Dual indexing**: Registers the event in both name-based and info-based hash tables

The function implements a double-checked locking pattern to handle concurrent access efficiently, first checking with a shared lock, then re-checking with an exclusive lock to prevent race conditions.

## Parameters / Member Variables
- : The wait event class identifier (e.g., PG_WAIT_EXTENSION, PG_WAIT_INJECTIONPOINT) that categorizes the type of wait event being created
- : A null-terminated string containing the name of the wait event. Must be less than NAMEDATALEN characters and unique within the specified class

## Dependencies
- Functions called/Symbols referenced:
  - strlen, strlcpy (string manipulation functions)
  - elog, ereport, errcode, errmsg (error reporting functions)
  - [LWLockAcquire](../L/LWLockAcquire.md), LWLockRelease (lightweight lock functions)
  - SpinLockAcquire, SpinLockRelease (spinlock functions)
  - [hash_search](../h/hash_search.md) (hash table search/insert function)
  - [pgstat_get_wait_event_type](../p/pgstat_get_wait_event_type.md) (wait event type name retrieval)
  - [WaitEventCustomEntryByName](WaitEventCustomEntryByName.md), WaitEventCustomEntryByInfo (hash entry types)
  - Various constants: NAMEDATALEN, WAIT_EVENT_CLASS_MASK, WAIT_EVENT_CUSTOM_HASH_MAX_SIZE
  - [Hash](../H/Hash.md) operation flags: HASH_FIND, HASH_ENTER
  - Lock modes: LW_SHARED, LW_EXCLUSIVE
  - Error codes: ERRCODE_DUPLICATE_OBJECT, ERRCODE_PROGRAM_LIMIT_EXCEEDED

- Called from (representative examples):
  - [WaitEventExtensionNew](WaitEventExtensionNew.md) (for extension wait events)
  - [WaitEventInjectionPointNew](WaitEventInjectionPointNew.md) (for injection point wait events)

## Notes and Other Information
- This is a static function, not directly accessible outside of wait_event.c
- Implements double-checked locking pattern for optimal performance in high-concurrency scenarios
- The function is atomic - either succeeds completely or fails without partial state
- Uses both lightweight locks (for hash table access) and spinlocks (for counter increment)
- Enforces a hard limit on the number of custom wait events (WAIT_EVENT_CUSTOM_HASH_MAX_SIZE)
- Returns a composite wait event info value combining class ID and unique event ID
- Event names must be unique within each class but can be reused across different classes
- The function handles the race condition where another process creates the same event between lock acquisition and release