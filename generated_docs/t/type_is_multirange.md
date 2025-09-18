# type_is_multirange

## Location
src/backend/utils/cache/lsyscache.c: 2698 - 2709

## Overview
A utility function that determines whether a given PostgreSQL type OID represents a multirange type.

## Definition
```c
bool type_is_multirange(Oid typid)
```

## Detailed Description
This function provides a simple boolean check to determine if a PostgreSQL type is a multirange type. Multirange types in PostgreSQL are data types that represent a set of non-overlapping ranges of some element type (e.g., int4multirange, tsmultirange). The function internally uses the type system cache to check if the type's typtype field is set to TYPTYPE_MULTIRANGE.

## Parameters / Member Variables
- `typid`: The OID (object identifier) of the PostgreSQL type to check

## Dependencies
- Functions called/Symbols referenced:
  - [get_typtype](../g/get_typtype.md)
  - TYPTYPE_MULTIRANGE
- Called from (representative examples):
  - [IsBinaryCoercibleWithCast](../I/IsBinaryCoercibleWithCast.md)
  - [multirange_agg_transfn](../m/multirange_agg_transfn.md)
  - [multirange_intersect_agg_transfn](../m/multirange_intersect_agg_transfn.md)

## Notes and Other Information
This function is part of PostgreSQL's system cache utilities (lsyscache.c) and provides a clean abstraction for type checking. Multirange types were introduced in PostgreSQL 14 as an extension of range types, allowing for collections of non-overlapping ranges. This function is commonly used in contexts where multirange-specific operations need to be performed or where type coercion rules need to be applied differently for multirange types.