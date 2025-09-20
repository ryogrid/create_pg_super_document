# lseg_construct

## Location
[src/backend/utils/adt/geo_ops.c:2129-2141](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L2129-L2141)

## Overview
Creates a line segment (LSEG) from two Point arguments, serving as a constructor function for the LSEG geometric data type.

## Definition
```c
Datum lseg_construct(PG_FUNCTION_ARGS)
```

## Detailed Description
The `lseg_construct` function is a PostgreSQL function that constructs a line segment from two Point parameters. It allocates memory for a new LSEG structure and delegates the actual construction logic to the `statlseg_construct` helper function. This function serves as the public SQL-callable interface for creating line segments from point coordinates, enabling users to construct line segments programmatically from point data in SQL queries. The function follows PostgreSQL's standard function interface pattern and memory management practices.

## Parameters / Member Variables
- Parameter 0: Point pointer - the first endpoint of the line segment
- Parameter 1: Point pointer - the second endpoint of the line segment

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_POINT_P` - macro to extract Point arguments (called twice)
  - [palloc](../p/palloc.md) - PostgreSQL memory allocation function
  - [statlseg_construct](../s/statlseg_construct.md) - helper function that performs the actual line segment construction
  - `PG_RETURN_LSEG_P` - macro to return LSEG pointer result
- Data types used:
  - [Point](../P/Point.md) - geometric point data type
  - [LSEG](../L/LSEG.md) - line segment data type

## Notes and Other Information
- This function acts as a wrapper around `statlseg_construct`, providing a SQL-callable interface
- Memory allocation is handled by the calling function, while construction logic is delegated
- Follows PostgreSQL's pattern of separating public function interfaces from internal implementation
- Can be used in SQL queries to create line segments from point values
- Part of PostgreSQL's geometric data type system for spatial calculations