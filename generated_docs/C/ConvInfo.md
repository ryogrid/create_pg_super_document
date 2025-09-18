# ConvInfo

## Location
src/bin/pg_dump/pg_dump.h: 293 - 294

## Overview
ConvInfo represents conversion objects in PostgreSQL's pg_dump utility, storing information about character set conversions that need to be dumped and restored.

## Definition


## Detailed Description
ConvInfo is a structure used by pg_dump to encapsulate information about conversion objects stored in the pg_conversion system catalog. It extends the base DumpableObject structure to include conversion-specific metadata required for dumping and restoring conversions. The structure is populated by the getConversions() function during the schema discovery phase and later used by dumpConversion() to generate the appropriate CREATE CONVERSION statements. Conversions define mappings between different character encodings in PostgreSQL.

## Parameters / Member Variables
- : Base DumpableObject containing common dump metadata (object ID, name, namespace, dependencies, etc.)
- : Owner role name of the conversion object, retrieved from pg_conversion.conowner

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject
  - getRoleName
  - findNamespace
  - AssignDumpId
  - selectDumpableObject
- Called from (representative examples):
  - getConversions (src/bin/pg_dump/pg_dump.c:6178)
  - dumpConversion (src/bin/pg_dump/pg_dump.c:14099)
  - fmtQualifiedDumpable (src/bin/pg_dump/pg_dump.c:249)

## Notes and Other Information
- ConvInfo objects are allocated as arrays in getConversions() function based on the number of conversions found in pg_conversion
- The structure inherits all functionality from DumpableObject including dependency tracking and selective dumping
- Unlike CollInfo, ConvInfo does not store encoding information directly as the conversion-specific details (source/target encodings, conversion function) are retrieved during the dump phase
- Used exclusively within the pg_dump utility for backup and restore operations
- System-defined conversions are filtered out during the dump process