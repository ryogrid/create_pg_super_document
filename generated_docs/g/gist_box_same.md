# gist_box_same

## Location
src/backend/access/gist/gistproc.c: 852 - 871

## Overview
A PostgreSQL function that implements the equality method for GiST indexes on geometric data types, testing whether two bounding boxes are exactly identical.

## Definition
```c
Datum gist_box_same(PG_FUNCTION_ARGS)
```

## Detailed Description
The `gist_box_same` function is the equality method used by GiST (Generalized Search Tree) indexes for geometric data types including boxes, points, circles, and polygons. This function is a critical component of GiST's internal operations, particularly for maintaining index consistency during searches and updates.

The function performs exact equality comparison between two bounding boxes by comparing all four coordinate values (low.x, low.y, high.x, high.y) using PostgreSQL's `float8_eq` function. Importantly, this function does NOT use fuzzy comparisons or tolerance-based equality checks, unlike the regular `box_same()` function used elsewhere in PostgreSQL.

This strict equality requirement is essential for maintaining GiST index consistency. Using fuzzy comparisons could lead to inconsistent results during index traversal and potentially break the index structure.

The function handles NULL values correctly by considering two NULL boxes as equal and any combination of NULL and non-NULL boxes as not equal.

## Parameters / Member Variables
- Function uses PostgreSQL's `PG_FUNCTION_ARGS` macro which provides:
  - First argument: Pointer to first BOX structure (b1)
  - Second argument: Pointer to second BOX structure (b2) 
  - Third argument: Pointer to boolean result location

## Dependencies
- Functions called/Symbols referenced:
  - [BOX](../B/BOX.md) (geometric box data structure)
  - PG_GETARG_BOX_P (macro to extract box pointer from function arguments)
  - [float8_eq](../f/float8_eq.md) (exact double-precision floating-point equality comparison)
  - PG_RETURN_POINTER (macro to return pointer result)
- Called from (representative examples):
  - No direct references found (likely called via GiST operator class function pointers)

## Notes and Other Information
- This function is used across multiple geometric data types (boxes, points, circles, polygons) since they all store bounding boxes as GiST index entries
- The strict equality check (no fuzzy comparison) is critical for index consistency - using tolerance-based comparisons could break the index structure
- Properly handles NULL input cases by considering two NULLs as equal
- The function is part of the GiST operator class interface and is typically called indirectly through function pointers rather than direct invocation
- Returns a pointer to boolean result rather than the boolean value directly, following PostgreSQL's function call conventions
- The exact equality semantics differ from the user-facing `box_same()` function which may use different comparison logic
- Essential for GiST internal operations like determining when index entries are identical during tree maintenance operations