# type_is_range

## Location
src/backend/utils/cache/lsyscache.c: 2688 - 2697

## Overview
A utility function that determines whether a given PostgreSQL type OID represents a range type.

## Definition
```c
bool type_is_range(Oid typid)
```

## Detailed Description
This function provides a simple boolean check to determine if a PostgreSQL type is a range type. Range types in PostgreSQL are data types that represent a range of values of some element type (e.g., int4range, tsrange). The function internally uses the type system cache to check if the type's typtype field is set to TYPTYPE_RANGE.

## Parameters / Member Variables
- `typid`: The OID (object identifier) of the PostgreSQL type to check

## Dependencies
- Functions called/Symbols referenced:
  - [get_typtype](../g/get_typtype.md)
  - TYPTYPE_RANGE
- Called from (representative examples):
  - [ExecAlterExtensionContentsRecurse](../E/ExecAlterExtensionContentsRecurse.md)
  - [IsBinaryCoercibleWithCast](../I/IsBinaryCoercibleWithCast.md)
  - [range_agg_transfn](../r/range_agg_transfn.md)
  - [range_intersect_agg_transfn](../r/range_intersect_agg_transfn.md)

## Notes and Other Information
This function is part of PostgreSQL's system cache utilities (lsyscache.c) and provides a clean abstraction for type checking. It's commonly used in contexts where range-specific operations need to be performed or where type coercion rules need to be applied differently for range types.