# _WriteLOData

## Location
src/bin/pg_dump/pg_backup_null.c: 92 - 109

## Overview
A specialized data writing function for the null archive format that converts large object (LO) binary data into SQL INSERT statements with bytea literals.

## Definition
```c
static void _WriteLOData(ArchiveHandle *AH, const void *data, size_t dLen)
```

## Detailed Description
_WriteLOData is a specialized function used by the null archive format to handle large object data writing. Unlike regular _WriteData, this function converts binary data into SQL statements that can be executed to recreate the large object content. It creates a bytea literal from the binary data and generates a SELECT statement using pg_catalog.lowrite() to write the data to the large object with handle 0. This function substitutes for _WriteData specifically when emitting large object data in the null format, which outputs directly readable SQL rather than a binary archive format.

## Parameters / Member Variables
- `AH`: Pointer to the ArchiveHandle containing the archive context
- `data`: Pointer to the binary data buffer to be converted to SQL
- `dLen`: Size in bytes of the data to be processed

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer (PostgreSQL buffer creation)
  - appendByteaLiteralAHX (converts binary to bytea literal)
  - ahprintf (formatted output to archive)
  - destroyPQExpBuffer (buffer cleanup)
- Called from (representative examples):
  - _StartLO (sets this as the active WriteData function)

## Notes and Other Information
- This function is specific to the null archive format implementation
- Creates SQL statements that use pg_catalog.lowrite(0, ...) to write large object data  
- Uses a temporary PQExpBuffer to safely format the bytea literal
- Only processes data when dLen > 0 to avoid generating empty statements
- The function substitutes _WriteData temporarily during large object emission
- The hardcoded handle '0' in the lowrite call suggests this is used in a specific large object context