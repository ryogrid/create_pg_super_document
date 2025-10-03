# EstimateUncommittedEnumsSpace

## Location
[src/backend/catalog/pg_enum.c:813-826](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_enum.c#L813-L826)

## Overview
Calculates the size required to serialize uncommitted enum types and values for parallel query processing.

## Definition
```c
Size EstimateUncommittedEnumsSpace(void)
```

## Detailed Description
This function estimates the memory space needed to serialize information about uncommitted enum types and enum values that have been created or modified within the current transaction. It is used in the context of parallel query processing where worker processes need access to uncommitted enum information from the leader process.

The function counts the number of entries in both the uncommitted_enum_types and uncommitted_enum_values hash tables, then calculates the space needed to store their OIDs plus two terminator entries. This size estimation is used by the parallel query infrastructure to allocate sufficient shared memory for serialization.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [hash_get_num_entries](../h/hash_get_num_entries.md) (to get count of hash table entries)
  - uncommitted_enum_types (global hash table variable)
  - uncommitted_enum_values (global hash table variable)
- Called from (representative examples):
  - [InitializeParallelDSM](../I/InitializeParallelDSM.md) (at src/backend/access/transam/parallel.c:288)
  - [SerializeUncommittedEnums](../S/SerializeUncommittedEnums.md) (at src/backend/catalog/pg_enum.c:835)

## Notes and Other Information
- Returns the size in bytes needed for serialization
- The calculation includes space for two terminator entries (hence entries + 2)
- This is part of PostgreSQL's parallel query infrastructure
- The function is safe to call even when the hash tables are NULL (uninitialized)
- The returned size is used to allocate shared memory segments for parallel workers

## Simplified Source

```c
Size EstimateUncommittedEnumsSpace(void) {
    size_t total_entries = 0;

    // Count uncommitted enum types
    if (uncommitted_enum_types)
        total_entries += hash_get_num_entries(uncommitted_enum_types);

    // Count uncommitted enum values
    if (uncommitted_enum_values)
        total_entries += hash_get_num_entries(uncommitted_enum_values);

    // Return space for entries plus two terminators
    return sizeof(Oid) * (total_entries + 2);
}
```