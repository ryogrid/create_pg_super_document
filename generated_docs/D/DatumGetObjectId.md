# DatumGetObjectId

## Location
[src/include/postgres.h:242-251](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/postgres.h#L242-L251)

## Overview
DatumGetObjectId is a static inline function that extracts an object identifier (Oid) value from a Datum, serving as a type conversion utility in PostgreSQL's internal data representation system.

## Definition

```c
static inline Oid
DatumGetObjectId(Datum X)
```
## Detailed Description
DatumGetObjectId performs a simple type cast from a Datum to an Oid (Object Identifier). This function is part of PostgreSQL's datum conversion interface, which provides consistent methods for extracting typed values from the generic Datum representation. The function performs no validation or transformation - it simply casts the input Datum directly to an Oid type. This is a zero-cost abstraction that enhances code readability and type safety when working with object identifiers stored as Datums.

## Parameters / Member Variables
- : A Datum value that contains an object identifier to be extracted

## Dependencies
- Functions called/Symbols referenced:
  - (None - performs direct cast)
- Called from (representative examples):
  - [btoidfastcmp](../b/btoidfastcmp.md) (B-tree comparison for OIDs)
  - [ExecGrant_common](../E/ExecGrant_common.md) (privilege checking)
  - [object_aclmask_ext](../o/object_aclmask_ext.md) (access control)
  - [find_expr_references_walker](../f/find_expr_references_walker.md) (dependency analysis)
  - [defGetObjectId](../d/defGetObjectId.md) (definition parsing)
  - PG_GETARG_OID (function argument extraction)

## Notes and Other Information
- This is a static inline function defined in src/include/postgres.h, making it available throughout the codebase
- Used extensively in access control, catalog operations, and type system functions
- Part of the family of DatumGet* conversion functions that provide type-safe extraction from Datum values
- The function assumes the input Datum actually contains a valid Oid value - no type checking is performed
- Commonly used in conjunction with PG_GETARG_OID macro for extracting OID arguments from PostgreSQL functions