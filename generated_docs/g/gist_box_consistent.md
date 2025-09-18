# gist_box_consistent

## Location
[src/backend/access/gist/gistproc.c:113-145](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistproc.c#L113-L145)

## Overview
The GiST Consistent method for box data types that determines whether index entries should be visited during query processing by delegating to appropriate leaf or internal consistency checking functions.

## Definition
```c
Datum gist_box_consistent(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the GiST (Generalized Search Tree) consistent method for BOX data types. It serves as the entry point for consistency checking during index searches, determining whether a given index entry could potentially contain data items that satisfy a query predicate.

The function acts as a dispatcher that:
1. Extracts query parameters from PostgreSQL's function call interface
2. Performs null checks on entry keys and query boxes
3. Determines whether the current entry is a leaf or internal node
4. Delegates to the appropriate specialized consistency checking function:
   - For leaf nodes: calls gist_box_leaf_consistent
   - For internal nodes: calls rtree_internal_consistent

The consistent method is fundamental to GiST index operation, as it prunes the search tree by eliminating subtrees that cannot possibly contain matching results.

## Parameters / Member Variables
- `entry`: GISTENTRY pointer containing the index entry being tested
- `query`: BOX pointer representing the query box to test against
- `strategy`: StrategyNumber indicating the type of spatial operation (overlaps, contains, etc.)
- `recheck`: Boolean pointer set to indicate whether exact checking is needed (always set to false for boxes)

## Dependencies
- Functions called/Symbols referenced:
  - [GISTENTRY](../G/GISTENTRY.md) (GiST entry structure)
  - [BOX](../B/BOX.md) (box data type)
  - PG_GETARG_BOX_P (macro for extracting box arguments)
  - StrategyNumber (enumeration for query strategies)
  - PG_GETARG_UINT16 (macro for extracting integer arguments)
  - [DatumGetBoxP](../D/DatumGetBoxP.md) (macro for converting Datum to BOX pointer)
  - GIST_LEAF (macro to check if entry is a leaf node)
  - [gist_box_leaf_consistent](gist_box_leaf_consistent.md) (for leaf-level consistency checking)
  - [rtree_internal_consistent](../r/rtree_internal_consistent.md) (for internal-level consistency checking)
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's function manager system)

## Notes and Other Information
- This is a PostgreSQL function following the PG_FUNCTION_ARGS calling convention
- The function always sets *recheck = false, indicating that all spatial operations on boxes are computed exactly
- Part of the GiST access method implementation for spatial indexing
- The function handles both leaf and internal node consistency checking through delegation
- Null safety is built-in with explicit checks for NULL entry keys and query boxes
- Located in src/backend/access/gist/gistproc.c:113-145
- This function would typically be registered in PostgreSQL's operator class for box GiST indexes
- The strategy parameter corresponds to spatial predicates like overlaps (&amp;&amp;), contains (@&gt;), within (&lt;@), etc.