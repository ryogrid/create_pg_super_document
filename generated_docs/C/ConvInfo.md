# ConvInfo

## Location
[src/bin/pg_dump/pg_dump.h:293-294](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L293-L294)

## Overview
ConvInfo represents conversion objects in PostgreSQL's pg_dump utility, storing information about character set conversions that need to be dumped and restored.

## Definition

```c
typedef struct _convInfo
{
	DumpableObject dobj;
	const char *rolname;
} ConvInfo;
```
## Detailed Description
ConvInfo is a structure used by pg_dump to encapsulate information about conversion objects stored in the pg_conversion system catalog. It extends the base DumpableObject structure to include conversion-specific metadata required for dumping and restoring conversions. The structure is populated by the getConversions() function during the schema discovery phase and later used by dumpConversion() to generate the appropriate CREATE CONVERSION statements. Conversions define mappings between different character encodings in PostgreSQL.

## Parameters / Member Variables
- `dobj`: Base DumpableObject containing common dump metadata (object ID, name, namespace, dependencies, etc.)
- `*rolname`: Owner role name of the conversion object, retrieved from pg_conversion.conowner
## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject
  - [getRoleName](../g/getRoleName.md)
  - [findNamespace](../f/findNamespace.md)
  - [AssignDumpId](../A/AssignDumpId.md)
  - [selectDumpableObject](../s/selectDumpableObject.md)
- Called from (representative examples):
  - [getConversions](../g/getConversions.md) (src/bin/pg_dump/pg_dump.c:6178)
  - [dumpConversion](../d/dumpConversion.md) (src/bin/pg_dump/pg_dump.c:14099)
  - fmtQualifiedDumpable (src/bin/pg_dump/pg_dump.c:249)

## Notes and Other Information
- [ConvInfo](ConvInfo.md) objects are allocated as arrays in getConversions() function based on the number of conversions found in pg_conversion
- The structure inherits all functionality from DumpableObject including dependency tracking and selective dumping
- Unlike CollInfo, ConvInfo does not store encoding information directly as the conversion-specific details (source/target encodings, conversion function) are retrieved during the dump phase
- Used exclusively within the pg_dump utility for backup and restore operations
- System-defined conversions are filtered out during the dump process