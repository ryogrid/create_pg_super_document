# TupleHashTableData

## Location
[src/include/nodes/execnodes.h:818-836](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L818-L836)

## Overview
TupleHashTableData is the core data structure that implements a hash table specifically designed for efficient tuple storage, lookup, and manipulation in PostgreSQLs execution engine.

## Definition
```c
typedef struct TupleHashTableData
{
    tuplehash_hash *hashtab;        /* underlying hash table */
    int             numCols;        /* number of columns in lookup key */
    AttrNumber     *keyColIdx;      /* attr numbers of key columns */
    FmgrInfo       *tab_hash_funcs; /* hash functions for table datatype(s) */
    ExprState      *tab_eq_func;    /* comparator for table datatype(s) */
    Oid            *tab_collations; /* collations for hash and comparison */
    MemoryContext   tablecxt;       /* memory context containing table */
    MemoryContext   tempcxt;        /* context for function evaluations */
    Size            entrysize;      /* actual size to make each hash entry */
    TupleTableSlot *tableslot;      /* slot for referencing table entries */
    /* The following fields are set transiently for each table search: */
    TupleTableSlot *inputslot;      /* current input tuples slot */
    FmgrInfo       *in_hash_funcs;  /* hash functions for input datatype(s) */
    ExprState      *cur_eq_func;    /* comparator for input vs. table */
    uint32          hash_iv;        /* hash-function IV */
    ExprContext    *exprcontext;    /* expression context */
} TupleHashTableData;
```

## Detailed Description
TupleHashTableData implements a specialized hash table optimized for PostgreSQLs tuple-based operations. This structure provides the foundation for efficient grouping, joining, and set operations within the executor. The design separates persistent table configuration from transient search state, enabling optimal performance for repeated operations.

The hash table integrates deeply with PostgreSQLs type system through configurable hash and comparison functions, supporting type-specific optimizations and proper collation handling. Memory management is carefully controlled through dedicated contexts, ensuring efficient allocation and cleanup.

The structure supports both table-resident data (stored permanently in the hash table) and input data (used temporarily during lookups), with separate function pointers and metadata for each. This dual-mode design optimizes performance when input and table data types differ or when specialized input processing is beneficial.

## Parameters / Member Variables
- `hashtab`: Pointer to the underlying hash table implementation that stores the actual entries
- `numCols`: Number of columns that comprise the lookup key for this hash table
- `keyColIdx`: Array of attribute numbers identifying which columns from tuples form the hash key
- `tab_hash_funcs`: Array of hash function info structures for computing hash values of table data types
- `tab_eq_func`: Expression state for comparing tuples stored in the table for equality
- `tab_collations`: Array of collation OIDs used for hash computation and comparison of table data
- `tablecxt`: Memory context that contains the hash table and its persistent data structures
- `tempcxt`: Memory context used for temporary allocations during function evaluations
- `entrysize`: Size in bytes of each hash table entry, including both header and tuple data
- `tableslot`: TupleTableSlot used for accessing and manipulating tuples stored in the table
- `inputslot`: TupleTableSlot for the current input tuple being processed (set per operation)
- `in_hash_funcs`: Hash function info for input tuple data types (may differ from table types)
- `cur_eq_func`: Expression state for comparing input tuples against table tuples
- `hash_iv`: Hash function initialization vector used for the current operation
- `exprcontext`: Expression context for evaluating comparison and hash functions

## Dependencies
- Functions called/Symbols referenced:
  - tuplehash_hash (underlying hash implementation)
  - AttrNumber (column attribute numbers)
  - [FmgrInfo](../F/FmgrInfo.md) (function manager info)
  - ExprState (expression evaluation state)
  - [MemoryContext](../M/MemoryContext.md) (memory management)
  - TupleTableSlot (tuple access interface)
  - ExprContext (expression evaluation context)
- Called from (representative examples):
  - BuildTupleHashTableExt (src/backend/executor/execGrouping.c:180)
  - [TupleHashTable](TupleHashTable.md) typedef (src/include/nodes/execnodes.h:800)

## Notes and Other Information
- The separation of persistent and transient fields enables efficient reuse of hash tables across multiple operations
- Hash function and comparison function configurability allows optimization for specific data types and operations
- Memory context separation ensures proper cleanup and prevents memory leaks in long-running operations
- The entrysize field accounts for variable-length tuple data and alignment requirements
- Expression evaluation integration enables complex multi-column keys with type conversion and function calls
- Used extensively in aggregation, set operations, and join processing where hash-based tuple organization is beneficial
- The hash_iv field supports salted hashing to prevent hash collision attacks in adversarial scenarios