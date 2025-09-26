# ResetTupleHashTable

## Location
[src/backend/executor/execGrouping.c:283-303](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execGrouping.c#L283-L303)

## Overview
Resets the contents of a TupleHashTable to be empty while preserving all non-content state and structure.

## Definition
```c
void ResetTupleHashTable(TupleHashTable hashtable);
```

## Detailed Description
This function efficiently clears all entries from a TupleHashTable while maintaining the hash table's structure, metadata, and configuration. It delegates the actual reset operation to the underlying tuplehash_reset() function which handles the low-level hash table clearing. The function preserves the hash table's bucket structure, function pointers, memory contexts, and other configuration parameters, making it ready for reuse without the overhead of complete reconstruction. Note that for proper memory management, the tablecxt memory context should also be reset separately to prevent memory leaks.

## Parameters / Member Variables
- `hashtable`: The TupleHashTable to reset - must be a valid hashtable previously created with BuildTupleHashTable or BuildTupleHashTableExt

## Dependencies
- Functions called/Symbols referenced:
  - tuplehash_reset (underlying hash table reset implementation)
- Called from (representative examples):
  - build_hash_tables (in aggregate node operations)
  - agg_refill_hash_table (during aggregate processing)
  - ExecReScanRecursiveUnion (recursive union rescanning)
  - ExecReScanSetOp (set operation rescanning)
  - buildSubPlanHash (subplan hash operations)

## Notes and Other Information
- Only clears the hash table contents, not the table structure or metadata
- The caller is responsible for resetting the tablecxt memory context to prevent leaks
- Works only with hashtables created using BuildTupleHashTableExt() for leak-free operation
- Hashtables created with the legacy BuildTupleHashTable() cannot be reset leak-free due to mixed memory contexts
- Much more efficient than destroying and recreating the hash table
- Commonly used in scenarios where the same hash table structure is reused across multiple iterations or rescans
- Essential for operations that need to clear and repopulate hash tables during query execution