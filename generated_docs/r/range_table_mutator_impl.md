# range_table_mutator_impl

## Location
[src/backend/nodes/nodeFuncs.c:3841-3909](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/nodeFuncs.c#L3841-L3909)

## Overview
A core PostgreSQL function that performs tree mutation operations specifically on a query's range table, supporting the broader query tree mutation infrastructure.

## Definition


## Detailed Description
The  function is the implementation core of the range table mutation functionality in PostgreSQL's node processing system. It iterates through each Range Table Entry (RTE) in a query's range table and applies mutation operations based on the RTE type. This function is split out from the larger  to provide focused range table processing that can be used independently.

The function handles different types of range table entries (relations, subqueries, joins, functions, table functions, values, CTEs, named tuple stores, and result relations) with type-specific mutation logic. It uses the  macro to recursively apply the mutation callback to relevant sub-structures while respecting control flags that may skip certain mutation operations.

## Parameters / Member Variables
- : The input range table (List of RangeTblEntry nodes) to be mutated
- : Callback function pointer that defines the specific mutation operations to apply
- : Opaque context pointer passed through to the mutator callback function
- : Control flags that modify mutation behavior (e.g., QTW_IGNORE_RT_SUBQUERIES, QTW_IGNORE_JOINALIASES)

## Dependencies
- Functions called/Symbols referenced:
  - FLATCOPY (macro for shallow copying structures)
  - MUTATE (macro for recursive mutation operations)
  - copyObject (deep copy function for unchanged substructures)
  - lappend (list append function)
- Called from (representative examples):
  - range_table_mutator (wrapper function)
  - planstate_tree_walker (indirectly via wrapper)

## Notes and Other Information
- The function performs a shallow copy () of each RangeTblEntry before mutation to avoid modifying the original structure
- Different RTE types require different mutation strategies: relations process table samples, subqueries recursively mutate the subquery tree, joins handle alias variables, etc.
- Security qualifiers () are always mutated regardless of RTE type
- Control flags allow callers to skip mutation of specific substructures (useful for optimization or when certain parts should remain unchanged)
- Located in src/backend/nodes/nodeFuncs.c:3841-3909