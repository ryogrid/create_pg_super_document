# btequalimage

## Location
src/backend/utils/adt/datum.c: 397 - 411

## Overview
A generic "equalimage" support function for B-Tree operator classes that can safely use binary image equality comparisons for optimization purposes.

## Definition
```c
Datum btequalimage(PG_FUNCTION_ARGS)
```

## Detailed Description
The `btequalimage` function serves as a generic support function (support function 4) for B-Tree operator classes. It indicates whether the operator class can safely use binary image equality comparisons instead of type-specific equality functions. This enables various B-Tree optimizations, particularly deduplication.

The function currently returns `true` unconditionally, meaning any B-Tree operator class that registers this function as its support function 4 is declaring that `datum_image_eq()` can safely replace its regular equality function in all cases. This allows PostgreSQL to use more efficient binary comparisons for operations like duplicate detection and removal.

The design includes provisions for future flexibility - if it becomes necessary to rescind support for specific operator classes, this could be done in a targeted fashion using the `opcintype` argument (which is currently commented out).

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro
  - `opcintype`: (commented out) OID of the operator class input type that could be used for targeted support decisions

## Dependencies
- Functions called/Symbols referenced:
  - PG_RETURN_BOOL (PostgreSQL macro for returning boolean values)
- Called from (representative examples):
  - (No direct references found - likely used through B-Tree operator class registration)

## Notes and Other Information
- This function is part of the B-Tree access method support infrastructure
- The unconditional `true` return value indicates full support for image equality optimizations
- Future versions could implement conditional logic based on the operator class type
- Used in conjunction with B-Tree deduplication and other optimization features
- The function signature follows PostgreSQL's V1 calling convention for support functions