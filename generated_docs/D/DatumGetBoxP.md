# DatumGetBoxP

## Location
[src/include/utils/geo_decls.h:234-238](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/geo_decls.h#L234-L238)

## Overview
DatumGetBoxP is a static inline function that extracts a BOX pointer from a PostgreSQL Datum value, providing type-safe access to geometric box data stored within the database's internal format.

## Definition

```c
static inline BOX *
DatumGetBoxP(Datum X)
```
## Detailed Description
This function serves as a type conversion utility within PostgreSQL's geometric data type system. It takes a generic Datum (PostgreSQL's universal data container) and safely casts it to a BOX pointer using DatumGetPointer. The function is implemented as a static inline for optimal performance since it's frequently used in geometric operations and indexing. The BOX type represents a rectangular box defined by two corner points in 2D space.

## Parameters / Member Variables
- `X`: A Datum value containing a pointer to BOX data that needs to be extracted and type-cast
## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetPointer](DatumGetPointer.md) (implicit through casting)
  - [BOX](../B/BOX.md) (geometric data type)
- Called from (representative examples):
  - [gist_box_consistent](../g/gist_box_consistent.md)
  - [gist_box_union](../g/gist_box_union.md)  
  - [gist_box_penalty](../g/gist_box_penalty.md)
  - [gist_box_picksplit](../g/gist_box_picksplit.md)
  - [gist_point_fetch](../g/gist_point_fetch.md)
  - [spg_box_quad_choose](../s/spg_box_quad_choose.md)

## Notes and Other Information
This function is part of PostgreSQL's geometric data type conversion infrastructure, primarily used by GiST (Generalized Search Tree) and SP-GiST (Space-Partitioned Generalized Search Tree) indexing methods for efficient spatial queries. It's defined in src/include/utils/geo_decls.h:234-238 and is extensively used throughout the geometric indexing subsystem.

## Simplified Source

```c
static inline BOX *
DatumGetBoxP(Datum X)
{
    // Convert generic Datum to BOX pointer
    // Used to extract BOX data from PostgreSQL's internal format
    return (BOX *) DatumGetPointer(X);
}
```