# _WriteLOData

## Location
[src/bin/pg_dump/pg_backup_null.c:92-109](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_null.c#L92-L109)

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
  - [createPQExpBuffer](../c/createPQExpBuffer.md) (PostgreSQL buffer creation)
  - appendByteaLiteralAHX (converts binary to bytea literal)
  - [ahprintf](../a/ahprintf.md) (formatted output to archive)
  - [destroyPQExpBuffer](../d/destroyPQExpBuffer.md) (buffer cleanup)
- Called from (representative examples):
  - [_StartLO](../S/_StartLO.md) (sets this as the active WriteData function)

## Notes and Other Information
- This function is specific to the null archive format implementation
- Creates SQL statements that use pg_catalog.lowrite(0, ...) to write large object data  
- Uses a temporary PQExpBuffer to safely format the bytea literal
- Only processes data when dLen > 0 to avoid generating empty statements
- The function substitutes _WriteData temporarily during large object emission
- The hardcoded handle '0' in the lowrite call suggests this is used in a specific large object context

## Simplified Source

```c
static void _WriteLOData(ArchiveHandle *AH, const void *data, size_t dLen)
{
    // Only process if there's actual data to write
    if (dLen > 0) {
        // Create buffer for SQL formatting
        PQExpBuffer buf = createPQExpBuffer();

        // Convert binary data to bytea literal format
        appendByteaLiteralAHX(buf, (const unsigned char *) data, dLen, AH);

        // Generate SQL statement to write large object data
        ahprintf(AH, "SELECT pg_catalog.lowrite(0, %s);\n", buf->data);

        // Clean up buffer
        destroyPQExpBuffer(buf);
    }
}
```