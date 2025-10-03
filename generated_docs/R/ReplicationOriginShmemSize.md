# ReplicationOriginShmemSize

## Location
[src/backend/replication/logical/origin.c:506-525](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/origin.c#L506-L525)

## Overview
Calculates the amount of shared memory required for replication origin state tracking based on the configured maximum number of replication slots.

## Definition
```c
Size ReplicationOriginShmemSize(void)
```

## Detailed Description
This function computes the shared memory size needed to store replication origin state information. The calculation is based on the max_replication_slots configuration parameter, which is reused here to determine the maximum number of replication origins that might need state tracking simultaneously.

The function calculates memory for:
1. The ReplicationStateCtl control structure header (up to the states array offset)
2. An array of ReplicationState structures, one for each potential replication slot

The design choice to use max_replication_slots is noted as arguably imperfect since this tracks remote transaction replay state rather than local replication slots, but it provides a reasonable upper bound without requiring an additional GUC parameter.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [add_size](../a/add_size.md): Safely adds sizes with overflow checking
  - [mul_size](../m/mul_size.md): Safely multiplies sizes with overflow checking
  - `offsetof`: Calculates offset of states field in ReplicationStateCtl structure
  - `max_replication_slots`: Global configuration variable for maximum replication slots
  - `[ReplicationStateCtl](ReplicationStateCtl.md)`: Control structure type for replication state management
  - `[ReplicationState](ReplicationState.md)`: Individual replication origin state structure type
- Called from (representative examples):
  - [ReplicationOriginShmemInit](ReplicationOriginShmemInit.md): During shared memory initialization to allocate the calculated amount
  - [CalculateShmemSize](../C/CalculateShmemSize.md): As part of total shared memory size calculation during PostgreSQL startup

## Notes and Other Information
- Returns 0 if max_replication_slots is 0, indicating no shared memory needed for replication origins
- Uses safe arithmetic functions (add_size, mul_size) to prevent integer overflow
- The comment acknowledges that max_replication_slots may not be the ideal metric but is sufficient for current needs
- This is part of the shared memory sizing phase that occurs during PostgreSQL startup before actual memory allocation
- The calculated size is used later by ReplicationOriginShmemInit to allocate and initialize the shared memory segment

## Simplified Source

```c
// Simplified version of ReplicationOriginShmemSize
Size ReplicationOriginShmemSize(void) {
    Size total_size = 0;

    // No memory needed if replication slots are disabled
    if (max_replication_slots == 0) {
        return 0;
    }

    // Calculate size for control structure header
    total_size = add_size(total_size, offsetof(ReplicationStateCtl, states));

    // Calculate size for array of replication state entries
    Size states_array_size = mul_size(max_replication_slots, sizeof(ReplicationState));
    total_size = add_size(total_size, states_array_size);

    return total_size;
}
```

Key simplifications made:
- Used more descriptive variable names (total_size instead of size)
- Added explicit early return for the zero case with clear comment
- Broke down the states array size calculation into a separate variable for clarity
- Added comments explaining each step of the memory calculation
- Preserved all essential logic and safety checks