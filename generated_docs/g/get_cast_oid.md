# get_cast_oid

## Location
src/backend/utils/cache/lsyscache.c: 1007 - 1034

## Overview
Looks up and returns the OID of a cast object that converts between two specified data types in the PostgreSQL type system.

## Definition
```c
Oid get_cast_oid(Oid sourcetypeid, Oid targettypeid, bool missing_ok)
```

## Detailed Description
The `get_cast_oid` function searches the PostgreSQL system catalog for a cast that can convert from one data type to another. It uses the system cache mechanism to efficiently locate the cast entry in the pg_cast catalog table. The function can operate in two modes: strict mode (where missing casts cause an error) and permissive mode (where missing casts return InvalidOid).

This function is essential for PostgreSQL's type conversion system, allowing the database to determine if and how values can be converted between different data types. The cast system supports explicit casts (requiring explicit CAST syntax), assignment casts (implicit in assignments), and implicit casts (automatic in expressions).

## Parameters / Member Variables
- `sourcetypeid`: The OID of the source data type from which to cast
- `targettypeid`: The OID of the target data type to which to cast  
- `missing_ok`: Boolean flag controlling error behavior - if false, throws an error when cast not found; if true, returns InvalidOid

## Dependencies
- Functions called/Symbols referenced:
  - GetSysCacheOid2 (performs system cache lookup for cast)
  - ObjectIdGetDatum (converts OID parameters to Datum format)
  - OidIsValid (checks if returned OID is valid)
  - ereport (reports errors when cast not found)
  - format_type_be (formats type names for error messages)
- Called from (representative examples):
  - get_object_address (when resolving cast object addresses)

## Notes and Other Information
- Returns InvalidOid when no cast exists and missing_ok is true
- Throws a detailed error with formatted type names when cast is missing and missing_ok is false
- Uses the CASTSOURCETARGET cache for efficient lookups based on source and target type OIDs
- Part of PostgreSQL's comprehensive type conversion and casting infrastructure
- The function only finds explicit cast entries; it does not handle implicit conversions that might be performed through I/O functions or other mechanisms