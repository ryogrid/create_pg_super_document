# TupleHashTable

## Location
[src/include/nodes/execnodes.h:800-801](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L800-L801)

## Overview
TupleHashTable is a typedef pointer to TupleHashTableData, representing a hash table specifically designed for tuple storage and lookup operations in PostgreSQLs executor subsystem.

## Definition
```c
typedef struct TupleHashTableData *TupleHashTable;
```

## Detailed Description
TupleHashTable serves as a handle to a hash table data structure optimized for storing and retrieving tuples based on specified key columns. This abstraction is extensively used throughout PostgreSQLs executor for operations requiring efficient tuple grouping, joining, and deduplication. The underlying TupleHashTableData structure contains all the necessary components for hash-based tuple operations including hash functions, comparison functions, memory contexts, and table slots.

The hash table is designed to work with PostgreSQLs tuple representation and integrates seamlessly with the executors slot-based tuple processing model. It supports configurable hash and equality functions based on the data types of the key columns, enabling type-specific optimizations.

## Parameters / Member Variables
This is a typedef pointer, so it references the members of TupleHashTableData:
- `hashtab`: Underlying hash table implementation
- `numCols`: Number of columns in the lookup key
- `keyColIdx`: Attribute numbers of key columns
- `tab_hash_funcs`: Hash functions for table datatypes
- `tab_eq_func`: Comparator for table datatypes
- `tab_collations`: Collations for hash and comparison operations
- `tablecxt`: Memory context containing the table
- `tempcxt`: Context for function evaluations
- `entrysize`: Actual size of each hash entry
- `tableslot`: Slot for referencing table entries
- `inputslot`: Current input tuples slot (transient)
- `in_hash_funcs`: Hash functions for input datatypes (transient)
- `cur_eq_func`: Comparator for input vs. table (transient)
- `hash_iv`: Hash-function initialization vector (transient)
- `exprcontext`: Expression context (transient)

## Dependencies
- Functions called/Symbols referenced:
  - [TupleHashTableData](TupleHashTableData.md) (underlying struct)
- Called from (representative examples):
  - [BuildTupleHashTableExt](../B/BuildTupleHashTableExt.md) (src/backend/executor/execGrouping.c:165)
  - [ResetTupleHashTable](../R/ResetTupleHashTable.md) (src/backend/executor/execGrouping.c:283)
  - [LookupTupleHashEntry](../L/LookupTupleHashEntry.md) (src/backend/executor/execGrouping.c:304)
  - [TupleHashTableHash](TupleHashTableHash.md) (src/backend/executor/execGrouping.c:336)
  - [initialize_hash_entry](../i/initialize_hash_entry.md) (src/backend/executor/nodeAgg.c:2045)
  - [lookup_hash_entries](../l/lookup_hash_entries.md) (src/backend/executor/nodeAgg.c:2104)

## Notes and Other Information
- Used extensively in aggregation operations (nodeAgg.c) for grouping tuples by key columns
- Essential component in set operations (SetOpState) for duplicate elimination
- Supports both hash-based lookups and sequential access patterns
- Memory management is handled through dedicated memory contexts to ensure proper cleanup
- The transient fields are set per-search operation to optimize repeated lookups with the same input characteristics
- Integrates with PostgreSQLs expression evaluation framework for complex key comparisons