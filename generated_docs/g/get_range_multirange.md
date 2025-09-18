# get_range_multirange

## Location
src/backend/utils/cache/lsyscache.c: 3458 - 3482

## Overview
Returns the multirange type corresponding to a given range type, enabling conversion between range and multirange types in PostgreSQL's type system.

## Definition


## Detailed Description
This function performs a lookup in the PostgreSQL system catalog to find the multirange type that corresponds to a given range type. Range types and multirange types are paired in PostgreSQL's type system - every range type has a corresponding multirange type that can hold multiple non-overlapping ranges of the same element type.

The function queries the pg_range system catalog using the provided range type OID and extracts the rngmultitypid field, which contains the OID of the corresponding multirange type. If the provided OID does not correspond to a valid range type, the function returns InvalidOid.

## Parameters / Member Variables
- : The OID of the range type for which to find the corresponding multirange type

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1 (system catalog lookup)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (tuple data extraction)
  - ReleaseSysCache (cache cleanup)
  - Form_pg_range (pg_range catalog structure)
  - ObjectIdGetDatum (OID to Datum conversion)
  - InvalidOid (constant for invalid OID)
- Called from (representative examples):
  - ExecAlterExtensionContentsRecurse
  - AlterTypeOwnerInternal
  - enforce_generic_type_consistency
  - resolve_anymultirange_from_others

## Notes and Other Information
- Returns InvalidOid if the input is not a valid range type OID
- Uses system cache for efficient catalog lookups
- Part of PostgreSQL's range/multirange type infrastructure
- Essential for type resolution in polymorphic function contexts involving anymultirange types
- The function assumes proper cache management and releases the cache tuple after use