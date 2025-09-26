# NamedLWLockTranche

## Location
[src/include/storage/lwlock.h:77-81](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/lwlock.h#L77-L81)

## Overview
NamedLWLockTranche is a structure that stores metadata about lightweight lock tranches, providing human-readable names for lock groups used in debugging and statistics.

## Definition

```c
typedef struct NamedLWLockTranche
{
	int			trancheId;
	char	   *trancheName;
} NamedLWLockTranche;
```
## Detailed Description
NamedLWLockTranche is a metadata structure that associates human-readable names with tranche IDs for lightweight locks. Tranches are groups of related LWLocks that serve similar purposes, and this structure enables PostgreSQL to provide meaningful names in debugging output, statistics, and monitoring tools. The structure is primarily used during system initialization to register tranche names that can later be referenced when reporting lock statistics or debugging lock contention issues.

## Parameters / Member Variables
- `trancheId`: Unique integer identifier for the lock tranche
- `trancheName`: Human-readable string name describing the purpose of locks in this tranche

## Dependencies
- Functions called/Symbols referenced:
  - (None - simple data structure)
- Called from (representative examples):
  - [LWLockShmemSize](../L/LWLockShmemSize.md) (calculating shared memory requirements)
  - [InitializeLWLocks](../I/InitializeLWLocks.md) (setting up lock tranche names during startup)

## Notes and Other Information
- Used primarily for debugging and monitoring purposes to provide meaningful names for lock tranches
- Helps administrators and developers identify which subsystems are experiencing lock contention
- The tranche names are typically descriptive strings like "Buffer Mapping", "Lock Manager", etc.
- Structure is populated during PostgreSQL initialization when lock tranches are registered
- Essential for lock statistics and wait event monitoring in production systems