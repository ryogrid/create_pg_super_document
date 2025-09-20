# InjectionPointsCtl

## Location
[src/backend/utils/misc/injection_point.c:82-86](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/injection_point.c#L82-L86)

## Overview
InjectionPointsCtl is the main control structure for managing an array of active injection points in shared memory, with optimization for efficient scanning.

## Definition

```c
typedef struct InjectionPointsCtl
{
	pg_atomic_uint32 max_inuse;
	InjectionPointEntry entries[MAX_INJECTION_POINTS];
} InjectionPointsCtl;
```
## Detailed Description
InjectionPointsCtl serves as the central management structure for PostgreSQL's injection point system. It maintains a shared memory array of all active injection points and includes an optimization field to avoid unnecessary scanning of the entire array when few or no injection points are active.

The structure is designed to be accessed from multiple processes in a PostgreSQL cluster, residing in shared memory. The max_inuse field provides a performance optimization by tracking the highest index currently in use plus one, allowing operations to limit their scanning to only the active portion of the array.

## Parameters / Member Variables
- `max_inuse`: Atomic 32-bit counter representing the highest index currently in use plus one. Used as an optimization to avoid scanning the entire entries array when most slots are unused.
- `entries[MAX_INJECTION_POINTS]`: Fixed-size array of InjectionPointEntry structures, with a maximum capacity of 128 injection points (MAX_INJECTION_POINTS).
## Dependencies
- Functions called/Symbols referenced:
  - [pg_atomic_uint32](../p/pg_atomic_uint32.md)
  - [InjectionPointEntry](InjectionPointEntry.md)
  - MAX_INJECTION_POINTS (128)
- Called from (representative examples):
  - [InjectionPointShmemSize](InjectionPointShmemSize.md)
  - [InjectionPointShmemInit](InjectionPointShmemInit.md)

## Notes and Other Information
The structure is allocated in shared memory and must be accessed using appropriate atomic operations for the max_inuse field. The fixed array size of 128 injection points provides a reasonable upper bound for the testing and debugging scenarios where injection points are typically used. The max_inuse optimization is particularly important in production systems where injection points are rarely used, allowing fast early exit from scanning operations.