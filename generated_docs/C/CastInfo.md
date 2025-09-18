# CastInfo

## Location
src/bin/pg_dump/pg_dump.h: 515 - 516

## Overview
CastInfo is a structure used by pg_dump to store metadata about type casts during the dump and restore process.

## Definition
```c
typedef struct _castInfo
{
    DumpableObject dobj;
    Oid         castsource;
    Oid         casttarget;
    Oid         castfunc;
    char        castcontext;
    char        castmethod;
} CastInfo;
```

## Detailed Description
CastInfo represents type cast metadata in PostgreSQL's pg_dump utility. It extends the base DumpableObject structure to include cast-specific information necessary for dumping and restoring type conversion operations. The structure stores information about the source and target types, the conversion function (if any), and the casting context and method used for the conversion.

## Parameters / Member Variables
- `dobj`: Base DumpableObject containing common dump metadata (OID, name, etc.)
- `castsource`: OID of the source data type for the cast
- `casttarget`: OID of the target data type for the cast
- `castfunc`: OID of the function used to perform the cast (0 if no function needed)
- `castcontext`: Character indicating the casting context (e=explicit, a=assignment, i=implicit)
- `castmethod`: Character indicating the casting method (f=function, i=inout, b=binary compatible)

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject (base structure)
- Called from (representative examples):
  - [selectDumpableCast](../s/selectDumpableCast.md) (src/bin/pg_dump/pg_dump.c:1976)
  - [getCasts](../g/getCasts.md) (src/bin/pg_dump/pg_dump.c:8604, 8640)
  - [dumpCast](../d/dumpCast.md) (src/bin/pg_dump/pg_dump.c:12728)
  - fmtQualifiedDumpable (src/bin/pg_dump/pg_dump.c:242)
  - [describeDumpableObject](../d/describeDumpableObject.md) (src/bin/pg_dump/pg_dump_sort.c:1608, 1609)

## Notes and Other Information
- Cast contexts determine when the cast can be invoked: explicit (only with CAST or :: syntax), assignment (during assignment), or implicit (automatically)
- Cast methods indicate how the conversion is performed: function-based, input/output function reuse, or binary compatible
- A castfunc value of 0 indicates no function is needed (typically for binary compatible casts)
- This structure is essential for maintaining type conversion capabilities when restoring database schemas
- The structure helps preserve the complete type system behavior across dump/restore operations