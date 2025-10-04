# BuildTupleHashTable

## Location
[src/backend/executor/execGrouping.c:253-282](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execGrouping.c#L253-L282)

## Overview
A backwards-compatibility wrapper function that creates a TupleHashTable by calling BuildTupleHashTableExt with simplified memory context management.

## Definition
```c
TupleHashTable BuildTupleHashTable(PlanState *parent,
                                  TupleDesc inputDesc,
                                  int numCols, AttrNumber *keyColIdx,
                                  const Oid *eqfuncoids,
                                  FmgrInfo *hashfunctions,
                                  Oid *collations,
                                  long nbuckets, Size additionalsize,
                                  MemoryContext tablecxt,
                                  MemoryContext tempcxt,
                                  bool use_variable_hash_iv);
```

## Detailed Description
This function serves as a simplified interface to BuildTupleHashTableExt for backwards compatibility. It allocates the hashtable's metadata in the same memory context as the table entries (tablecxt), unlike the extended version which allows separate contexts for metadata and table data. This design choice means that hashtables created with this function cannot be reset in a leak-free manner using ResetTupleHashTable(), as the metadata and table data are intermingled in the same memory context.

## Parameters / Member Variables
- `parent`: PlanState node that will own this hash table
- `inputDesc`: TupleDesc describing the structure of input tuples
- `numCols`: Number of columns to use as lookup keys
- `keyColIdx`: Array of column indices (AttrNumber) that serve as hash keys
- `eqfuncoids`: Array of equality function OIDs for key comparisons
- `hashfunctions`: Array of FmgrInfo structures for hashing functions
- `collations`: Array of collation OIDs for each key column
- `nbuckets`: Initial estimate of hash table size
- `additionalsize`: Size of additional data stored in each hash entry
- `tablecxt`: Memory context for both metadata and table entries
- `tempcxt`: Short-lived context for evaluating hash and comparison functions
- `use_variable_hash_iv`: Whether to use variable hash initialization vector for parallel processing

## Dependencies
- Functions called/Symbols referenced:
  - [BuildTupleHashTableExt](BuildTupleHashTableExt.md) (the extended implementation that does the actual work)
- Called from (representative examples):
  - Referenced in executor header files as part of the public API

## Notes and Other Information
- This is purely a wrapper function that delegates to BuildTupleHashTableExt
- Sets both metacxt and tablecxt parameters to the same tablecxt value
- Cannot be reset leak-free with ResetTupleHashTable() due to shared memory context
- Provided for backward compatibility with existing code that expects the simpler interface
- New code should prefer BuildTupleHashTableExt for better memory management control
- The function signature is identical to BuildTupleHashTableExt except for the missing metacxt parameter

## Simplified Source

```c
TupleHashTable BuildTupleHashTable(PlanState *parent,
                                  TupleDesc inputDesc,
                                  int numCols, AttrNumber *keyColIdx,
                                  const Oid *eqfuncoids,
                                  FmgrInfo *hashfunctions,
                                  Oid *collations,
                                  long nbuckets, Size additionalsize,
                                  MemoryContext tablecxt,
                                  MemoryContext tempcxt,
                                  bool use_variable_hash_iv) {
    // Backwards-compatibility wrapper - delegates to extended version
    // Uses same memory context for both metadata and table data
    return BuildTupleHashTableExt(parent,
                                 inputDesc,
                                 numCols, keyColIdx,
                                 eqfuncoids,
                                 hashfunctions,
                                 collations,
                                 nbuckets, additionalsize,
                                 tablecxt,    // metacxt parameter
                                 tablecxt,    // tablecxt parameter
                                 tempcxt,
                                 use_variable_hash_iv);
}
```