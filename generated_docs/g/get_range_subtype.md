# get_range_subtype

## Location
src/backend/utils/cache/lsyscache.c: 3407 - 3432

## Overview
Returns the subtype OID of a given PostgreSQL range type.

## Definition
```c
Oid get_range_subtype(Oid rangeOid)
```

## Detailed Description
The get_range_subtype function retrieves the subtype (element type) of a PostgreSQL range type from the pg_range system catalog. Range types in PostgreSQL are composite types that represent a range of values of a particular base type (the subtype). For example, an int4range has int4 as its subtype, and a tsrange has timestamp as its subtype. This function performs a system cache lookup to efficiently retrieve the subtype OID for a given range type OID.

## Parameters / Member Variables
- `rangeOid`: The OID (Object Identifier) of the range type whose subtype is to be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system cache lookup using RANGETYPE cache)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (macro to extract structure from heap tuple)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (cache reference cleanup)
  - Form_pg_range (structure type for pg_range catalog)
- Called from (representative examples):
  - [CheckAttributeType](../C/CheckAttributeType.md)
  - [check_generic_type_consistency](../c/check_generic_type_consistency.md)
  - [enforce_generic_type_consistency](../e/enforce_generic_type_consistency.md)
  - [resolve_anyelement_from_others](../r/resolve_anyelement_from_others.md)

## Notes and Other Information
- Returns InvalidOid if the provided OID does not correspond to a range type
- Part of the PG_RANGE CACHES section in lsyscache.c
- Essential for type system operations involving range types and polymorphic functions
- Used primarily in type checking and coercion logic
- The subtype information is stored in the rngsubtype field of the pg_range system catalog
- Critical for proper handling of polymorphic functions that work with range types
- Enables the type system to understand the relationship between range types and their element types