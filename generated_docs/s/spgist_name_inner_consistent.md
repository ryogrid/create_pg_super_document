# spgist_name_inner_consistent

## Location
[src/test/modules/spgist_name_ops/spgist_name_ops.c:266-398](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/spgist_name_ops/spgist_name_ops.c#L266-L398)

## Overview
Implements the inner consistent function for SP-GiST name operator class, evaluating which child nodes should be visited during index traversal based on search predicates.

## Definition
```c
Datum spgist_name_inner_consistent(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is part of the SP-GiST (Space-Partitioned Generalized Search Tree) implementation for PostgreSQL's name data type. It determines which child nodes of an inner tuple should be visited during index traversal by reconstructing partial values and comparing them against search conditions.

The function reconstructs text values by combining:
1. Previously reconstructed value from parent levels
2. Prefix data from the current tuple (if any)  
3. Node labels from child nodes

For each child node, it performs string comparison operations against all scan keys using the appropriate B-tree strategy (less than, equal, greater than, etc.) to determine consistency.

## Parameters / Member Variables
- : Input structure () containing:
  - : Value reconstructed from parent levels
  - : Length of previously reconstructed value
  - : Whether tuple has prefix data
  - : Prefix data if present
  - : Number of child nodes
  - : Labels for each child node
  - : Number of scan keys
  - : Array of scan key conditions
- : Output structure () to populate with:
  - : Array of consistent child node numbers
  - : Array of level increments for each node
  - : Array of reconstructed values
  - : Number of consistent nodes found

## Dependencies
- Functions called/Symbols referenced:
  - 
  - 
  - 
  - 
  - 
  - Strategy numbers (, , etc.)
- Called from (representative examples):
  - Referenced by  function

## Notes and Other Information
- Handles dummy node labels (values ≤ 0) by excluding them from reconstructed data
- Performs non-collation-aware string comparisons using 
- Assumes reconstructed values use long varlena format (not toasted or short headers)
- Returns void but populates output structure with results
- Part of test module demonstrating SP-GiST operator class implementation for name types