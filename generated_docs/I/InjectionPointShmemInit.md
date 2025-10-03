# InjectionPointShmemInit

## Location
[src/backend/utils/misc/injection_point.c:248-271](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/injection_point.c#L248-L271)

## Overview
Initializes shared memory space for the injection point system, setting up the dynamic shared hash table used to track active injection points across PostgreSQL processes.

## Definition

```c
void
InjectionPointShmemInit(void)
```
## Detailed Description
This function allocates and initializes shared memory for the injection point subsystem. It creates a shared memory structure called "InjectionPoint hash" that holds the global state for all injection points. The function behaves differently depending on whether it's running in the postmaster process or a child process:

- In the postmaster process (!IsUnderPostmaster): Creates new shared memory and initializes all atomic counters to zero
- In child processes: Attaches to existing shared memory that was already initialized by the postmaster

The initialization is conditional on the USE_INJECTION_POINTS compilation flag, making it a no-op when injection points are disabled.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [ShmemInitStruct](../S/ShmemInitStruct.md)
  - [pg_atomic_init_u32](../p/pg_atomic_init_u32.md)
  - [pg_atomic_init_u64](../p/pg_atomic_init_u64.md)
- Types referenced:
  - [InjectionPointsCtl](InjectionPointsCtl.md)
- Constants used:
  - MAX_INJECTION_POINTS
- Called from:
  - [CreateOrAttachShmemStructs](../C/CreateOrAttachShmemStructs.md) (src/backend/storage/ipc/ipci.c:359)

## Notes and Other Information
- Only functional when compiled with USE_INJECTION_POINTS defined
- Must be called during PostgreSQL startup as part of shared memory initialization
- The shared memory structure includes atomic counters for thread-safe access across multiple processes
- Uses assertions to verify correct initialization state between postmaster and child processes
- The global variable ActiveInjectionPoints is set to point to the shared memory structure

## Simplified Source

```c
// Simplified version of InjectionPointShmemInit
void InjectionPointShmemInit(void) {
#ifdef USE_INJECTION_POINTS
    bool found;

    // Step 1: Allocate or attach to shared memory structure
    ActiveInjectionPoints = ShmemInitStruct("InjectionPoint hash",
                                           sizeof(InjectionPointsCtl),
                                           &found);

    // Step 2: Initialize counters if we're the postmaster
    if (!IsUnderPostmaster) {
        // First time initialization - set up all atomic counters
        pg_atomic_init_u32(&ActiveInjectionPoints->max_inuse, 0);

        // Initialize all injection point entries
        for (int i = 0; i < MAX_INJECTION_POINTS; i++) {
            pg_atomic_init_u64(&ActiveInjectionPoints->entries[i].generation, 0);
        }
    }
    // Step 3: Child processes just verify shared memory exists
    // (Assertion checks removed for clarity)
#endif
}
```

Key simplifications made:
- Removed assertion checks for clarity while preserving core logic
- Added descriptive comments for each initialization step
- Consolidated the postmaster vs child process logic explanation
- Focused on the main execution path while noting the conditional compilation