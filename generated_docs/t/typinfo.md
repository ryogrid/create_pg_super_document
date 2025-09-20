# typinfo

## Location
[src/backend/bootstrap/bootstrap.c:74-142](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/bootstrap/bootstrap.c#L74-L142)

## Overview
The  struct contains basic information for PostgreSQL data types used during the bootstrap phase before the pg_type catalog is available.

## Definition

```c
struct typinfo
{
	char		name[NAMEDATALEN];
	Oid			oid;
	Oid			elem;
	int16		len;
	bool		byval;
	char		align;
	char		storage;
	Oid			collation;
	Oid			inproc;
	Oid			outproc;
};
```
## Detailed Description
The  structure is used during PostgreSQL's bootstrap process to provide essential type information for core data types before the system catalog  is fully initialized. This structure is part of the bootstrap system that creates the initial catalog tables. It contains all the fundamental properties needed to handle data types during the early database initialization phase, including type alignment, storage characteristics, and input/output functions.

The structure is used to populate a static array  that contains entries for all the basic PostgreSQL data types like bool, int2, int4, text, oid, etc. This allows the bootstrap process to create and manipulate catalog tables before the type system is fully operational.

## Parameters / Member Variables
- `name[NAMEDATALEN]`: Type name (limited to NAMEDATALEN characters)
- `oid`: Object identifier for the type
- `elem`: Element type OID for array types (InvalidOid for non-arrays)
- `len`: Type length in bytes (-1 for variable length types)
- `byval`: True if type is passed by value, false if passed by reference
- `align`: Type alignment requirement (TYPALIGN_CHAR, TYPALIGN_SHORT, TYPALIGN_INT)
- `storage`: Storage strategy (TYPSTORAGE_PLAIN, TYPSTORAGE_EXTENDED, etc.)
- `collation`: Default collation OID for the type (InvalidOid if not collatable)
- `inproc`: Input function OID for converting text to internal format
- `outproc`: Output function OID for converting internal format to text

## Dependencies
- Functions called/Symbols referenced:
  - NAMEDATALEN
  - TYPALIGN_* constants
  - TYPSTORAGE_* constants
- Called from (representative examples):
  - [DefineAttr](../D/DefineAttr.md) (via TypInfo array access at line 527-533)
  - getTypinfo (via TypInfo array lookup at line 782)
  - [boot_get_type_io_data](../b/boot_get_type_io_data.md) (via TypInfo array access at line 852-871)

## Notes and Other Information
This structure is only used during bootstrap and is replaced by the pg_type catalog once the system is fully initialized. The static TypInfo[] array contains hardcoded entries for approximately 22 fundamental PostgreSQL data types that are essential for creating the initial system catalogs. The structure provides a bridge between the bootstrap process and the full type system.