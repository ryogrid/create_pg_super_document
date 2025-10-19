# box_overlap

## Location
[src/backend/utils/adt/geo_ops.c:563-571](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L563-L571)

## Overview
PostgreSQL function that determines whether two BOX structures overlap by delegating to the internal box_ov function.

## Definition

```c
Datum
box_overlap(PG_FUNCTION_ARGS)
```
## Detailed Description
The `box_overlap` function serves as the PostgreSQL SQL-callable interface for testing box overlap operations. It acts as a thin wrapper around the internal `box_ov` function, providing the standard PostgreSQL function calling convention while delegating the actual overlap computation logic. This function is used to implement the overlap operator (&&) for BOX data types in SQL queries.

The function follows PostgreSQL's standard pattern of extracting arguments, calling internal logic functions, and returning results in the proper format for the SQL engine.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function calling convention
  - Argument 0: BOX pointer - first box to test for overlap
  - Argument 1: BOX pointer - second box to test for overlap

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BOX_P (retrieves BOX arguments)
  - [box_ov](box_ov.md) (performs actual overlap calculation)
  - PG_RETURN_BOOL (returns boolean result)
  - [BOX](../B/BOX.md) (box data structure)
- Called from (representative examples):
  - [gist_box_leaf_consistent](../g/gist_box_leaf_consistent.md) (GiST index consistency checking)
  - [rtree_internal_consistent](../r/rtree_internal_consistent.md) (R-tree index operations)
  - [spg_box_quad_leaf_consistent](../s/spg_box_quad_leaf_consistent.md) (SP-GiST index consistency checking)

## Notes and Other Information
- This is the SQL-callable interface for the && (overlap) operator between BOX types
- The actual overlap logic is implemented in the `box_ov` function
- Widely used in PostgreSQL's spatial indexing systems (GiST, SP-GiST, R-tree)
- Essential for geometric queries involving spatial relationships
- Part of PostgreSQL's geometric operator system in geo_ops.c
- Enables SQL queries like: SELECT * FROM table WHERE box1 && box2

## Simplified Source

```c
Datum
box_overlap(PG_FUNCTION_ARGS)
{
    BOX *box1 = PG_GETARG_BOX_P(0);
    BOX *box2 = PG_GETARG_BOX_P(1);

    // Delegate to internal overlap function
    PG_RETURN_BOOL(box_ov(box1, box2));
}
```