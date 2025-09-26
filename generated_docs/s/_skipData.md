# _skipData

## Location
[src/bin/pg_dump/pg_backup_custom.c:623-668](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_custom.c#L623-L668)

## Overview
Skips data blocks from the current file position in a PostgreSQL custom format archive by either seeking past them or reading and discarding the content.

## Definition
```c
static void _skipData(ArchiveHandle *AH)
```

## Detailed Description
This function skips over data blocks in a PostgreSQL custom format dump file. Data blocks are formatted with an integer length field followed by the actual data. The function reads these length values sequentially and skips the corresponding data until it encounters a zero length, which indicates the end of the data blocks.

The function uses two different strategies for skipping data:
1. **Seek-based skipping**: If the archive supports seeking (ctx->hasSeek is true), it uses fseeko() to efficiently jump past the data
2. **Read-based skipping**: If seeking is not available, it allocates a buffer and reads the data into it (then discards it)

The function manages memory efficiently by reusing and growing the buffer as needed when in read mode.

## Parameters / Member Variables
- `AH`: Archive handle containing the state and context for the dump/restore operation, including the file handle and format-specific data

## Dependencies
- Functions called/Symbols referenced:
  - [lclContext](../l/lclContext.md): Local context structure for custom format handling
  - [ReadInt](../R/ReadInt.md): Reads integer length values from the archive
  - fseeko: Performs file seeking operations when available
  - [pg_malloc](../p/pg_malloc.md): Allocates memory for the read buffer
- Called from (representative examples):
  - [_PrintTocData](../P/_PrintTocData.md): Used during table of contents processing
  - [_skipLOs](_skipLOs.md): Used when skipping Large Object data blocks

## Notes and Other Information
- This is a static function specific to the custom format archive handling
- The function automatically chooses between seek and read strategies based on file capabilities
- Memory management includes proper cleanup with free() at the end
- Zero-length blocks serve as sentinel values to mark the end of data sections
- Error handling includes specific messages for EOF and general read errors
- File location: src/bin/pg_dump/pg_backup_custom.c:623-668