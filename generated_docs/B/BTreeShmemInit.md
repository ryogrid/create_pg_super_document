# BTreeShmemInit

## Location
[src/backend/access/nbtree/nbtutils.c:4535-4562](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtutils.c#L4535-L4562)

## Overview
Initializes the shared memory structures used for coordinating B-tree VACUUM operations across all PostgreSQL backends.

## Definition

```c
void
BTreeShmemInit(void)
```
## Detailed Description
This function sets up the shared memory infrastructure required for B-tree VACUUM coordination. It allocates and initializes the  structure that tracks all currently active VACUUM operations across the system. The function handles both the initial creation (in the postmaster process) and attachment (in child processes) scenarios.

During initialization, the function:
- Allocates shared memory using the size calculated by 
- Initializes the cycle counter with a semi-random value based on current time to avoid predictable patterns
- Sets up the vacuum tracking array with capacity for MaxBackends concurrent operations
- Performs appropriate assertions based on whether this is the initial setup or a child process attachment

The function uses PostgreSQL's shared memory initialization infrastructure and follows the standard pattern of checking the  flag to determine initialization vs. attachment behavior.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [ShmemInitStruct](../S/ShmemInitStruct.md) (shared memory allocation)
  - [BTreeShmemSize](BTreeShmemSize.md) (size calculation function)
  - time (for cycle counter initialization)
  - IsUnderPostmaster (process type check)
  - Assert (assertion macro)
  - MaxBackends (global configuration)
- Called from (representative examples):
  - [CreateOrAttachShmemStructs](../C/CreateOrAttachShmemStructs.md) (during system startup)

## Notes and Other Information
- Only the postmaster process performs actual initialization (!IsUnderPostmaster)
- Child processes only attach to existing shared memory (IsUnderPostmaster)
- Cycle counter is seeded with time() to provide semi-random starting values
- The max_vacuums field is set to MaxBackends to handle worst-case concurrent VACUUM scenarios
- Uses PostgreSQL's standard shared memory segment naming ("BTree Vacuum State")
- Critical for system startup - failure here would prevent B-tree VACUUM coordination

## Simplified Source

```c
// Simplified version of BTreeShmemInit
void BTreeShmemInit(void) {
    bool found;

    // Allocate/attach to shared memory for BTree vacuum coordination
    btvacinfo = (BTVacInfo *) ShmemInitStruct("BTree Vacuum State",
                                              BTreeShmemSize(),
                                              &found);

    // Initialize shared memory if this is the postmaster process
    if (!IsUnderPostmaster) {
        // First time initialization - should not already exist
        Assert(!found);

        // Initialize cycle counter with current time for randomness
        btvacinfo->cycle_ctr = (BTCycleId) time(NULL);

        // Set up vacuum tracking structure
        btvacinfo->num_vacuums = 0;
        btvacinfo->max_vacuums = MaxBackends;
    }
    else {
        // Child process - shared memory should already exist
        Assert(found);
    }
}
```

Key simplifications made:
- Removed detailed comments for brevity while preserving essential explanations
- Consolidated the initialization logic into clear sections
- Focused on the two main execution paths (postmaster vs child process)
- Maintained the essential assertion logic and initialization sequence