# get_typbyval

## Location
src/backend/utils/cache/lsyscache.c: 2222 - 2250

## Overview
Determines whether a PostgreSQL data type is passed by value or by reference, returning a boolean flag that controls how the type's data is handled in function calls and storage operations.

## Definition
```c
bool get_typbyval(Oid typid)
```

## Detailed Description
The `get_typbyval` function retrieves the `typbyval` attribute from the PostgreSQL type catalog for a given type OID. This boolean attribute determines the calling convention for the data type - whether values of this type are passed by value (copied directly) or by reference (passed as pointers). This distinction is crucial for PostgreSQL's internal data handling, memory management, and function call mechanisms.

Types that are passed by value are typically small, fixed-size types (like integers, booleans, small fixed-length types) that can fit in a Datum (typically 8 bytes on 64-bit systems). Types passed by reference include variable-length types, large fixed-length types, and any type that doesn't fit in a Datum.

This function is essential for the executor, type system, and serialization code to correctly handle data values according to their storage and passing conventions.

## Parameters / Member Variables
- `typid`: The OID (Object Identifier) of the PostgreSQL data type whose pass-by-value status is to be determined

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1 (system cache lookup)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (macro to extract struct from heap tuple)
  - ReleaseSysCache (cache cleanup)
  - Form_pg_type (type catalog structure)
- Called from (representative examples):
  - brin_range_serialize (BRIN index serialization)
  - brin_range_deserialize (BRIN index deserialization)

## Notes and Other Information
- Returns false if the type OID is invalid or not found in the catalog
- Pass-by-value types are typically limited to those that fit in a Datum (8 bytes on 64-bit, 4 bytes on 32-bit systems)
- Common pass-by-value types include: bool, int2, int4, float4, char, and small fixed-length types
- Common pass-by-reference types include: text, varchar, bytea, int8 (on 32-bit), numeric, arrays, and composite types
- This information is critical for proper memory management and avoiding data corruption when handling type values
- The function uses the system cache for performance optimization