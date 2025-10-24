# CloseArchive

## Location
[src/bin/pg_dump/pg_backup_archiver.c:252-265](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L252-L265)

## Overview
Properly closes a PostgreSQL dump archive, performing necessary cleanup operations and closing any associated file handles.

## Definition
```c
void CloseArchive(Archive *AHX)
```

## Detailed Description
The CloseArchive function performs the cleanup and finalization operations required when closing a PostgreSQL dump archive. It first calls the format-specific close function through the ClosePtr function pointer, allowing each archive format to perform its own specialized cleanup. After that, it handles the closing of the output file handle, ensuring proper compression stream termination if compression was used.

The function includes error handling to detect and report issues during the file closing process, using the system errno to provide detailed error information if the close operation fails.

## Parameters / Member Variables
- `AHX`: Pointer to the Archive structure to be closed

## Dependencies
- Functions called/Symbols referenced:
  - [EndCompressFileHandle](../E/EndCompressFileHandle.md)
  - [pg_fatal](../p/pg_fatal.md) (for error reporting)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_dump.c)
  - [main](../m/main.md) (in pg_restore.c)

## Notes and Other Information
- This is a public function in the pg_dump/pg_restore architecture
- The function performs a two-stage close: format-specific cleanup followed by file handle closure
- Error handling is implemented to catch and report file closing failures
- The function casts the Archive pointer to ArchiveHandle to access internal structure members
- Used by both pg_dump (for writing archives) and pg_restore (for reading archives)
- Proper cleanup is essential to ensure data integrity and prevent resource leaks

## Simplified Source

```c
void
CloseArchive(Archive *AHX)
{
    ArchiveHandle *AH = (ArchiveHandle *) AHX;

    // Call format-specific close function
    AH->ClosePtr(AH);

    // Close the output file handle
    errno = 0;
    if (!EndCompressFileHandle(AH->OF))
        pg_fatal("could not close output file: %m");
}
```