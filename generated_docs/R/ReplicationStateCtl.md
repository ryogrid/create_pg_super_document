# ReplicationStateCtl

## Location
src/backend/replication/logical/origin.c: 146 - 152

## Overview
ReplicationStateCtl is the control structure that manages an array of replication states for all logical replication origins in shared memory.

## Definition
```c
typedef struct ReplicationStateCtl
{
    int tranche_id;
    ReplicationState states[FLEXIBLE_ARRAY_MEMBER];
} ReplicationStateCtl;
```

## Detailed Description
The ReplicationStateCtl structure serves as the master control structure for managing all replication origin states in PostgreSQL. It is allocated in shared memory and contains an array of ReplicationState structures, one for each possible replication origin. This structure acts as the central repository for tracking the progress of all logical replication streams.

The tranche_id field is used for lock management, specifically for organizing the lightweight locks associated with each replication state into a logical group or "tranche." This helps the lock manager efficiently handle the per-origin locks. The states array uses the FLEXIBLE_ARRAY_MEMBER designation, meaning its size is determined at allocation time based on the max_replication_slots configuration parameter.

## Parameters / Member Variables
- `tranche_id`: Identifier for the lock tranche used for per-origin lightweight locks
- `states`: Flexible array of ReplicationState structures, sized according to max_replication_slots

## Dependencies
- Functions called/Symbols referenced:
  - ReplicationState (the individual replication state structure)
  - FLEXIBLE_ARRAY_MEMBER (macro for flexible array declaration)
- Called from (representative examples):
  - ReplicationOriginShmemSize
  - ReplicationOriginShmemInit

## Notes and Other Information
- This structure is allocated once during shared memory initialization
- The array size corresponds to the max_replication_slots configuration parameter
- Each element in the states array tracks one logical replication origin
- The tranche_id enables efficient lock management by grouping related locks together
- This structure provides the foundation for PostgreSQLs logical replication origin tracking system
- Access to individual replication states should be coordinated through the locks embedded in each ReplicationState