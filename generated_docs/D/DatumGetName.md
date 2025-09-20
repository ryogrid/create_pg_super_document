# DatumGetName

## Location
[src/include/postgres.h:360-372](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/postgres.h#L360-L372)

## Overview
DatumGetName extracts a Name value from a Datum, converting PostgreSQL's internal data representation back to the Name type for system catalog and identifier operations.

## Definition

```c
static inline Name
DatumGetName(Datum X)
```
## Detailed Description
DatumGetName is a conversion function that extracts a Name value from a Datum representation. It works by casting the result of DatumGetPointer() to the Name type. The Name type in PostgreSQL is a fixed-length string type (typically 64 bytes) used primarily for storing identifiers in system catalogs such as table names, column names, function names, etc.

This function is the inverse operation of NameGetDatum() and is essential for retrieving name values from Datum representations when working with system catalogs or processing identifier-related operations.

## Parameters / Member Variables
- : A Datum containing a Name value that will be extracted and returned as a Name type

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetPointer](DatumGetPointer.md)
  - Name (type cast)
- Called from (representative examples):
  - [ExecGrant_common](../E/ExecGrant_common.md) (access control)
  - pg_identify_object (object identification)
  - [GetSubscription](../G/GetSubscription.md) (subscription management)
  - [pg_get_triggerdef_worker](../p/pg_get_triggerdef_worker.md) (rule utilities)
  - [namefastcmp_c](../n/namefastcmp_c.md) (name comparison functions)
  - PG_GETARG_NAME (function manager macro)

## Notes and Other Information
- The Name type is a fixed-length string type used for PostgreSQL identifiers
- Names are typically limited to NAMEDATALEN-1 characters (usually 63 characters)
- This function assumes the Datum contains a valid Name pointer
- Used extensively in system catalog operations and identifier processing
- The returned Name is a pointer to the actual name data structure, not a copy
- Care should be taken to ensure the underlying data remains valid while the Name is in use