# RestoreUncommittedEnums

## Location
src/backend/catalog/pg_enum.c: 873 - 906

## Overview
Deserializes uncommitted enum types and values from a serialized buffer in parallel query worker processes.

## Definition
```c
void RestoreUncommittedEnums(void *space)
```

## Detailed Description
This function is the counterpart to SerializeUncommittedEnums and is responsible for deserializing uncommitted enum information in parallel query worker processes. It reads the serialized data format created by SerializeUncommittedEnums and reconstructs the uncommitted_enum_types and uncommitted_enum_values hash tables.

The function expects the serialized data to contain:
1. OIDs for uncommitted enum types, terminated by InvalidOid
2. OIDs for uncommitted enum values, terminated by InvalidOid

The function includes optimizations to avoid creating hash tables when no uncommitted enums exist (the common case). It also includes assertions to ensure the hash tables are initially empty, preventing double-restoration.

## Parameters / Member Variables
- `space`: Pointer to the memory buffer containing serialized enum data

## Dependencies
- Functions called/Symbols referenced:
  - [init_uncommitted_enum_types](../i/init_uncommitted_enum_types.md) (initializes uncommitted enum types hash table)
  - [init_uncommitted_enum_values](../i/init_uncommitted_enum_values.md) (initializes uncommitted enum values hash table)
  - [hash_search](../h/hash_search.md) (adds entries to hash tables)
  - HASH_ENTER (hash table operation flag)
  - OidIsValid (checks if OID is valid/not a terminator)
- Called from (representative examples):
  - [ParallelWorkerMain](../P/ParallelWorkerMain.md) (at src/backend/access/transam/parallel.c:1522)

## Notes and Other Information
- Includes assertions to ensure hash tables are initially NULL (not already initialized)
- Optimized for the common case where no uncommitted enums exist
- Part of PostgreSQL's parallel query infrastructure for sharing transaction state
- Must be called with data serialized by SerializeUncommittedEnums
- Creates hash tables only when needed (when valid OIDs are present)
- Uses InvalidOid as terminators to separate enum types from enum values
- The restored hash tables will be cleaned up automatically at transaction end