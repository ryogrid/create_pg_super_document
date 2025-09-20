# InjectionPointAttach

## Location
[src/backend/utils/misc/injection_point.c:272-359](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/injection_point.c#L272-L359)

## Overview
Registers a new injection point in the shared memory hash table, associating a name with a library, function, and optional private data for dynamic code injection during testing.

## Definition

```c
void
InjectionPointAttach(const char *name,
					 const char *library,
					 const char *function,
					 const void *private_data,
					 int private_data_size)
```
## Detailed Description
This function creates and registers a new injection point in PostgreSQL's testing infrastructure. It performs several validation checks on the input parameters, finds a free slot in the shared memory array, and atomically registers the new injection point. The function uses a generation counter mechanism to ensure thread-safe access to injection point entries.

The registration process involves:
1. Validating parameter lengths against defined maximums
2. Acquiring an exclusive lock on the injection point system
3. Searching for existing entries with the same name (prevents duplicates)
4. Finding a free slot in the shared memory array
5. Copying the injection point data into the entry
6. Atomically updating the generation counter to mark the entry as active

## Parameters / Member Variables
- : The unique identifier for the injection point (max length INJ_NAME_MAXLEN)
- : The dynamic library containing the injection function (max length INJ_LIB_MAXLEN)
- : The function name to call when the injection point is triggered (max length INJ_FUNC_MAXLEN)
- : Optional user-defined data passed to the injection function (can be NULL)
- : Size of the private data in bytes (max INJ_PRIVATE_MAXLEN)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_atomic_read_u32](../p/pg_atomic_read_u32.md)
  - [pg_atomic_read_u64](../p/pg_atomic_read_u64.md)
  - [pg_atomic_write_u64](../p/pg_atomic_write_u64.md)
  - [pg_atomic_write_u32](../p/pg_atomic_write_u32.md)
  - pg_write_barrier
  - strlcpy
  - LWLockAcquire/LWLockRelease
- Types referenced:
  - [InjectionPointEntry](InjectionPointEntry.md)
- Constants used:
  - INJ_NAME_MAXLEN
  - INJ_LIB_MAXLEN
  - INJ_FUNC_MAXLEN
  - INJ_PRIVATE_MAXLEN
  - MAX_INJECTION_POINTS
- Called from:
  - [injection_points_attach](../i/injection_points_attach.md) (src/test/modules/injection_points/injection_points.c:290)

## Notes and Other Information
- Only functional when compiled with USE_INJECTION_POINTS defined
- Uses exclusive locking (InjectionPointLock) to ensure thread-safe registration
- Employs generation counters where odd numbers indicate active entries and even numbers indicate free slots
- Prevents duplicate injection point names within the same PostgreSQL instance
- Will error if maximum number of injection points is exceeded
- Uses memory barriers to ensure proper ordering of memory operations
- Primarily used for testing and debugging purposes in PostgreSQL development