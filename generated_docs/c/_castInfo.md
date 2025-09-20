# _castInfo

## Location
[src/bin/pg_dump/pg_dump.h:507-514](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L507-L514)

## Overview
The  structure represents type cast information in PostgreSQL's pg_dump utility, storing metadata about how one data type can be converted to another.

## Definition

```c
typedef struct _castInfo
{
	DumpableObject dobj;
	Oid			castsource;
	Oid			casttarget;
	Oid			castfunc;
	char		castcontext;
	char		castmethod;
} CastInfo;
```
## Detailed Description
The  structure encapsulates all information necessary to define a type cast in PostgreSQL. It identifies the source and target types, the conversion function (if any), the context in which the cast can be applied, and the method used for the conversion. This structure enables pg_dump to preserve user-defined casts during database backup and restoration operations.

## Parameters / Member Variables
- `dobj`: Base  containing common metadata for dump operations
- `castsource`: OID of the source data type for the cast
- `casttarget`: OID of the target data type for the cast
- `castfunc`: OID of the function used to perform the cast (may be InvalidOid for binary-compatible casts)
- `castcontext`: Character indicating the context where the cast can be applied ('e' for explicit, 'a' for assignment, 'i' for implicit)
- `castmethod`: Character indicating the casting method ('f' for function, 'i' for inout, 'b' for binary compatible)
## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject
- Called from (representative examples):
  - No direct references found in current analysis

## Notes and Other Information
- Cast contexts determine when PostgreSQL will automatically apply the cast: explicit casts require explicit CAST() syntax, assignment casts are applied during assignments, and implicit casts are applied automatically in expressions
- Binary compatible casts (castmethod='b') do not require a conversion function as they operate on types with identical storage representations
- Function casts (castmethod='f') use the specified function to convert between types
- Input/output casts (castmethod='i') convert via text representation using the types' input and output functions
- This structure allows pg_dump to recreate custom cast definitions with their exact behavior during database restoration