# _DeClone

## Location
[src/bin/pg_dump/pg_backup_custom.c:905-916](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_custom.c#L905-L916)

## Overview
This function deallocates the format-specific context structure for the custom archive format during archive cleanup.

## Definition
```c
static void _DeClone(ArchiveHandle *AH)
```

## Detailed Description
_DeClone is a cleanup function specifically designed for the custom archive format in pg_dump. It is responsible for freeing the local context structure (lclContext) that was allocated during archive initialization. This function is part of the archive format interface and is called when cleaning up after parallel restore operations or when the archive handle is being destroyed. The function simply casts the formatData pointer back to lclContext and frees the allocated memory.

## Parameters / Member Variables
- `AH`: Archive handle containing the format-specific data to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - [lclContext](../l/lclContext.md) (structure type)
  - free (standard library function)
- Called from (representative examples):
  - [InitArchiveFmt_Custom](../I/InitArchiveFmt_Custom.md)
  - [InitArchiveFmt_Directory](../I/InitArchiveFmt_Directory.md) (through function pointer assignment)

## Notes and Other Information
- This is a static function internal to pg_backup_custom.c
- The function assumes that AH->formatData contains a valid lclContext pointer
- Part of the archive format interface for managing format-specific cleanup
- Used in conjunction with _Clone function for parallel restore operations
- The lclContext structure contains compression state, seek capability flag, and file position information