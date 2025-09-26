# execTuplesHashPrepare

## Location
[src/backend/executor/execGrouping.c:95-152](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execGrouping.c#L95-L152)

## Overview
Prepares equality and hash functions needed for TupleHashTable operations by looking up function OIDs and initializing FmgrInfo structures.

## Definition
```c
void execTuplesHashPrepare(int numCols,
                          const Oid *eqOperators,
                          Oid **eqFuncOids,
                          FmgrInfo **hashFunctions);
```

## Detailed Description
This function is similar to execTuplesMatchPrepare but additionally handles hash functions required for TupleHashTable operations. It takes equality operators and resolves them to both equality function OIDs and their corresponding hash function OIDs. The function allocates memory for the output arrays and initializes FmgrInfo structures for the hash functions, which are essential for efficient hash-based tuple operations. The function expects that the given operators are not cross-type comparisons and will assert this condition.

## Parameters / Member Variables
- `numCols`: Number of columns for which to prepare hash and equality functions
- `eqOperators`: Array of equality operator OIDs for each column
- `eqFuncOids`: Output parameter - receives allocated array of equality function OIDs
- `hashFunctions`: Output parameter - receives allocated array of initialized FmgrInfo structures for hash functions

## Dependencies
- Functions called/Symbols referenced:
  - [get_opcode](../g/get_opcode.md) (converts equality operator OID to function OID)
  - [get_op_hash_functions](../g/get_op_hash_functions.md) (retrieves hash functions for an operator)
  - [fmgr_info](../f/fmgr_info.md) (initializes FmgrInfo structure)
  - [palloc](../p/palloc.md) (memory allocation)
- Called from (representative examples):
  - [find_hash_columns](../f/find_hash_columns.md) (in aggregate node initialization)
  - [ExecInitRecursiveUnion](../E/ExecInitRecursiveUnion.md) (recursive union node setup)
  - [ExecInitSetOp](../E/ExecInitSetOp.md) (set operation node setup)

## Notes and Other Information
- Allocates memory for both eqFuncOids and hashFunctions arrays using palloc
- Does not support cross-type comparisons (left and right hash functions must be identical)
- Will throw an ERROR if hash functions cannot be found for any operator
- Essential for hash-based operations like hash joins, hash aggregation, and set operations
- The initialized FmgrInfo structures allow efficient function calls during query execution
- Related to TupleHashTable infrastructure for high-performance tuple hashing