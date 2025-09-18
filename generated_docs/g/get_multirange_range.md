# get_multirange_range

## Location
src/backend/utils/cache/lsyscache.c: 3483 - 3511

## Overview
Returns the range type corresponding to a given multirange type, enabling reverse conversion from multirange to range types in PostgreSQL's type system.

## Definition
```c
Oid get_multirange_range(Oid multirangeOid)
```

## Detailed Description
This function performs the inverse operation of get_range_multirange by looking up the range type that corresponds to a given multirange type. It queries the PostgreSQL system catalog using the RANGEMULTIRANGE cache to find the pg_range entry for the specified multirange type and extracts the rngtypid field, which contains the OID of the corresponding range type.

The function is essential for type resolution in PostgreSQL's polymorphic type system, particularly when working with generic types like anyelement, anyrange, and anymultirange. It allows the system to determine the underlying range type from a multirange type, enabling proper type checking and coercion.

## Parameters / Member Variables
- `multirangeOid`: The OID of the multirange type for which to find the corresponding range type

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1 (system catalog lookup with RANGEMULTIRANGE cache)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (tuple data extraction)
  - ReleaseSysCache (cache cleanup)
  - Form_pg_range (pg_range catalog structure)
  - ObjectIdGetDatum (OID to Datum conversion)
  - InvalidOid (constant for invalid OID)
- Called from (representative examples):
  - pg_type_aclmask_ext
  - AlterTypeOwner
  - check_generic_type_consistency
  - enforce_generic_type_consistency
  - load_multirangetype_info
  - resolve_anyelement_from_others
  - resolve_anyrange_from_others

## Notes and Other Information
- Returns InvalidOid if the input is not a valid multirange type OID
- Uses the RANGEMULTIRANGE system cache for efficient lookups
- Complementary to get_range_multirange function
- Critical for polymorphic function resolution involving multirange types
- Used extensively in type coercion and ACL checking
- Properly manages system cache resources by releasing cached tuples