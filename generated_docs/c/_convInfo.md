# _convInfo

## Location
[src/bin/pg_dump/pg_dump.h:289-292](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L289-L292)

## Overview
A structure definition used in PostgreSQL's pg_dump utility to represent character encoding conversion information for database dumping and restoration operations.

## Definition

```c
typedef struct _convInfo
{
	DumpableObject dobj;
	const char *rolname;
} ConvInfo;
```
## Detailed Description
The  structure is part of PostgreSQL's pg_dump utility framework, designed to store metadata about character encoding conversions during database backup operations. Character encoding conversions in PostgreSQL enable automatic translation between different character sets and encodings when data is transferred between databases with different encoding schemes. This structure extends the base  to include conversion-specific information, enabling pg_dump to properly serialize and restore character encoding conversion definitions along with their ownership information.

## Parameters / Member Variables
- : Base  structure containing common metadata for dumpable database objects (object ID, name, namespace, dump flags, etc.)
- : Pointer to constant string containing the name of the role (user) who owns this character encoding conversion

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject (base structure)
- Called from (representative examples):
  - [getConversions](../g/getConversions.md) (allocation and initialization of conversion arrays)
  - [dumpConversion](../d/dumpConversion.md) (for dumping character encoding conversion definitions)

## Notes and Other Information
- This structure is specifically used within the pg_dump utility context for backup and restore operations
- The structure is typedef'd as  for easier usage throughout the codebase
- Character encoding conversions are essential for databases that need to handle multiple character sets or migrate between different encoding schemes
- Conversions define the rules and procedures for translating text data from one character encoding to another (e.g., UTF8 to LATIN1, ASCII to UTF8, etc.)
- The  field preserves ownership information necessary for proper access control during database restoration
- Character encoding conversions are typically implemented using conversion functions that handle the actual transformation logic
- Part of PostgreSQL's internationalization support system, enabling databases to work with multiple character encodings simultaneously
- Conversions can be bidirectional or unidirectional depending on the compatibility between source and target encodings