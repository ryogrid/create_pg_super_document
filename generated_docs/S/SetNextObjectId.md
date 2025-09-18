# SetNextObjectId

## Location
[src/backend/access/transam/varsup.c:623-651](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/varsup.c#L623-L651)

## Overview
Sets the next Object Identifier (OID) counter to a specified value, exclusively used during database initialization (initdb) to establish the starting point for OID allocation.

## Definition
```c
static void SetNextObjectId(Oid nextOid)
```

## Detailed Description
This static function provides a controlled mechanism to advance the global OID counter during database initialization. It serves as a foundation function that allows initdb to establish specific OID starting points for different phases of database bootstrap.

The function implements strict safety checks to ensure it can only be used during the appropriate initialization phase:

**Environment Restriction**: Only callable during initdb (non-postmaster environment), preventing accidental or malicious OID counter manipulation in production.

**Monotonic Advancement**: Ensures the OID counter can only move forward, never backward, preventing conflicts with previously allocated OIDs.

**State Reset**: Resets the prefetch counter (`oidCount`) to 0, ensuring that the next OID allocation will trigger proper WAL logging through the prefetch mechanism.

The function is part of PostgreSQL's careful OID management during bootstrap, where specific OID ranges must be established for system catalogs and other bootstrap objects.

## Parameters / Member Variables
- `nextOid`: The target OID value to set as the next available OID; must be greater than or equal to the current `nextOid` value

## Dependencies
- Functions called/Symbols referenced:
  - `IsPostmasterEnvironment` (macro/variable check)
  - `elog` (ERROR level)
  - `LWLockAcquire` (OidGenLock, LW_EXCLUSIVE)
  - `LWLockRelease` (OidGenLock)
- Called from (representative examples):
  - `StopGeneratingPinnedObjectIds` (src/backend/access/transam/varsup.c:654)

## Notes and Other Information
- **Static function**: Not exposed outside varsup.c, indicating its specialized internal use
- **Initdb-only**: Strictly restricted to database initialization phase, cannot be used after postmaster startup
- **Validation**: Prevents setting the counter to a value lower than the current counter to maintain monotonic progression
- **Prefetch reset**: Sets `oidCount` to 0, forcing the next allocation to trigger WAL logging
- **Thread safety**: Uses exclusive locking even though only callable during single-threaded initdb (defensive programming)
- **Error handling**: Provides clear error messages for both environment and value validation failures
- **Bootstrap coordination**: Works with other OID management functions to establish proper OID ranges during database initialization