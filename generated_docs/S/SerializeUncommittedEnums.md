# SerializeUncommittedEnums

## Location
src/backend/catalog/pg_enum.c: 827 - 872

## Overview
Serializes uncommitted enum types and values into a memory buffer for parallel query worker processes.

## Definition
```c
void SerializeUncommittedEnums(void *space, Size size)
```

## Detailed Description
This function serializes the contents of the uncommitted_enum_types and uncommitted_enum_values hash tables into a contiguous memory buffer. It is used in PostgreSQL's parallel query processing to share information about uncommitted enum changes from the leader process to worker processes.

The serialization format consists of:
1. All OIDs from the uncommitted_enum_types hash table
2. A terminator (InvalidOid)
3. All OIDs from the uncommitted_enum_values hash table  
4. Another terminator (InvalidOid)

The function includes assertions to ensure the provided space matches the estimated size and that the actual serialized data fits exactly in the allocated space.

## Parameters / Member Variables
- `space`: Pointer to the memory buffer where serialized data will be written
- `size`: Size of the provided memory buffer (must match EstimateUncommittedEnumsSpace())

## Dependencies
- Functions called/Symbols referenced:
  - [EstimateUncommittedEnumsSpace](../E/EstimateUncommittedEnumsSpace.md) (for size validation)
  - HASH_SEQ_STATUS (hash table iteration structure)
  - [hash_seq_init](../h/hash_seq_init.md) (initialize hash table iteration)
  - [hash_seq_search](../h/hash_seq_search.md) (iterate through hash table entries)
  - uncommitted_enum_types (global hash table variable)
  - uncommitted_enum_values (global hash table variable)
- Called from (representative examples):
  - [InitializeParallelDSM](../I/InitializeParallelDSM.md) (at src/backend/access/transam/parallel.c:441)

## Notes and Other Information
- The function uses InvalidOid as terminators to separate enum types from enum values
- Includes debug assertions to validate size calculations and space usage
- Part of PostgreSQL's parallel query infrastructure for sharing transaction state
- Safe to call even when hash tables are NULL (will just write terminators)
- The serialized format must be compatible with RestoreUncommittedEnums for deserialization
- Hash table iteration order is not guaranteed to be consistent across calls