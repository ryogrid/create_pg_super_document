# NamedLWLockTrancheRequest

## Location
[src/backend/storage/lmgr/lwlock.c:212-216](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lwlock.c#L212-L216)

## Overview
NamedLWLockTrancheRequest is a structure that represents a request for creating a named tranche of lightweight locks (LWLocks), specifying the tranche name and the number of locks needed.

## Definition
```c
typedef struct NamedLWLockTrancheRequest
{
    char        tranche_name[NAMEDATALEN];
    int         num_lwlocks;
} NamedLWLockTrancheRequest;
```

## Detailed Description
NamedLWLockTrancheRequest is used to request the allocation of a named tranche of lightweight locks during PostgreSQL initialization. A tranche is a group of related locks that share common characteristics and are managed together. This structure allows subsystems to request a specific number of locks organized under a named tranche, which helps with lock identification, debugging, and management.

The structure is primarily used during the database startup process when various subsystems register their lock requirements. The tranche name provides a way to categorize and identify groups of locks, while the number specifies how many individual locks should be allocated for that tranche.

## Parameters / Member Variables
- `tranche_name`: A character array of size NAMEDATALEN containing the name identifier for the lock tranche
- `num_lwlocks`: An integer specifying the number of lightweight locks to allocate for this tranche

## Dependencies
- Functions called/Symbols referenced:
  - NAMEDATALEN (constant defining maximum name length)
- Called from (representative examples):
  - InitializeLWLocks
  - RequestNamedLWLockTranche

## Notes and Other Information
- This structure is used during PostgreSQL initialization to set up named lock tranches
- The tranche name is limited to NAMEDATALEN characters (typically 64 bytes including null terminator)
- Named tranches help organize locks logically and assist in debugging and monitoring
- Defined in src/backend/storage/lmgr/lwlock.c at lines 212-216
- Multiple references found in RequestNamedLWLockTranche function, indicating active usage in the lock management system