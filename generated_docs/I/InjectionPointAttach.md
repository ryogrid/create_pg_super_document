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
- `*name`: The unique identifier for the injection point (max length INJ_NAME_MAXLEN)
- `*library`: The dynamic library containing the injection function (max length INJ_LIB_MAXLEN)
- `*function`: The function name to call when the injection point is triggered (max length INJ_FUNC_MAXLEN)
- `*private_data`: Optional user-defined data passed to the injection function (can be NULL)
- `private_data_size`: Size of the private data in bytes (max INJ_PRIVATE_MAXLEN)
## Dependencies
- Functions called/Symbols referenced:
  - [pg_atomic_read_u32](../p/pg_atomic_read_u32.md)
  - [pg_atomic_read_u64](../p/pg_atomic_read_u64.md)
  - [pg_atomic_write_u64](../p/pg_atomic_write_u64.md)
  - [pg_atomic_write_u32](../p/pg_atomic_write_u32.md)
  - pg_write_barrier
  - [strlcpy](../s/strlcpy.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease
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

## Simplified Source

```c
void
InjectionPointAttach(const char *name,
                     const char *library,
                     const char *function,
                     const void *private_data,
                     int private_data_size)
{
#ifdef USE_INJECTION_POINTS
    // Validate input parameter lengths
    if (strlen(name) >= INJ_NAME_MAXLEN ||
        strlen(library) >= INJ_LIB_MAXLEN ||
        strlen(function) >= INJ_FUNC_MAXLEN ||
        private_data_size >= INJ_PRIVATE_MAXLEN)
        elog(ERROR, "injection point parameter too long");

    // Find free slot and check for duplicates
    LWLockAcquire(InjectionPointLock, LW_EXCLUSIVE);

    uint32 max_inuse = pg_atomic_read_u32(&ActiveInjectionPoints->max_inuse);
    int free_idx = -1;

    for (int idx = 0; idx < max_inuse; idx++) {
        InjectionPointEntry *entry = &ActiveInjectionPoints->entries[idx];
        uint64 generation = pg_atomic_read_u64(&entry->generation);

        // Even generation = free slot, odd = active
        if (generation % 2 == 0) {
            if (free_idx == -1) free_idx = idx;
        } else if (strcmp(entry->name, name) == 0) {
            elog(ERROR, "injection point \"%s\" already defined", name);
        }
    }

    // Expand array if no free slot found
    if (free_idx == -1) {
        if (max_inuse == MAX_INJECTION_POINTS)
            elog(ERROR, "too many injection points");
        free_idx = max_inuse;
    }

    // Fill entry with provided data
    InjectionPointEntry *entry = &ActiveInjectionPoints->entries[free_idx];
    strlcpy(entry->name, name, sizeof(entry->name));
    strlcpy(entry->library, library, sizeof(entry->library));
    strlcpy(entry->function, function, sizeof(entry->function));
    if (private_data != NULL)
        memcpy(entry->private_data, private_data, private_data_size);

    // Atomically activate entry (make generation odd)
    uint64 generation = pg_atomic_read_u64(&entry->generation);
    pg_write_barrier();
    pg_atomic_write_u64(&entry->generation, generation + 1);

    // Update max_inuse if needed
    if (free_idx + 1 > max_inuse)
        pg_atomic_write_u32(&ActiveInjectionPoints->max_inuse, free_idx + 1);

    LWLockRelease(InjectionPointLock);
#else
    elog(ERROR, "injection points are not supported by this build");
#endif
}
```