# GetWaitEventCustomIdentifier

## Location
[src/backend/utils/activity/wait_event.c:277-306](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/wait_event.c#L277-L306)

## Overview
Returns the name of a custom wait event based on the provided wait event information identifier.

## Definition


## Detailed Description
This static function retrieves the human-readable name string for a custom wait event given its numeric identifier. It first checks if the wait event is the built-in "Extension" event (PG_WAIT_EXTENSION), and if not, performs a hash table lookup to find the corresponding custom wait event name. The function uses shared locking on WaitEventCustomLock to ensure thread-safe access to the hash table during the lookup operation.

## Parameters / Member Variables
- : A 32-bit unsigned integer identifier for the wait event whose name is to be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire
  - [hash_search](../h/hash_search.md)
  - LWLockRelease
  - elog
- Data structures used:
  - WaitEventCustomEntryByInfo
  - WaitEventCustomHashByInfo
  - WaitEventCustomLock
- Constants used:
  - PG_WAIT_EXTENSION
  - LW_SHARED
  - HASH_FIND
- Called from (representative examples):
  - [pgstat_get_wait_event](../p/pgstat_get_wait_event.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the wait_event.c file
- The function will throw an ERROR if the requested wait event information cannot be found in the hash table
- Uses shared locking to allow concurrent reads while preventing writes during the lookup
- Handles both built-in extension events and user-defined custom events
- Located at src/backend/utils/activity/wait_event.c:277-306