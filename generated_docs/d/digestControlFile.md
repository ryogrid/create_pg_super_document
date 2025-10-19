# digestControlFile

## Location
[src/bin/pg_rewind/pg_rewind.c:1023-1055](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/pg_rewind.c#L1023-L1055)

## Overview
Verifies the contents of a PostgreSQL control file buffer and copies it to a ControlFileData structure, performing validation checks on the data.

## Definition

```c
static void
digestControlFile(ControlFileData *ControlFile, const char *content,
				  size_t size)
```
## Detailed Description
This function is used in pg_rewind to process control file data that has been read from either the source or target PostgreSQL cluster. It performs several critical validation steps:

1. Verifies that the control file size matches the expected PG_CONTROL_FILE_SIZE
2. Copies the raw control file content into the provided ControlFileData structure
3. Extracts and validates the WAL segment size from the control file
4. Performs additional control file validation through checkControlFile()

The function is essential for ensuring that pg_rewind operates on valid control file data before proceeding with the rewind operation.

## Parameters / Member Variables
- `*ControlFile`: Pointer to ControlFileData structure where the parsed control file data will be stored
- `*content`: Raw bytes of the control file content as read from disk
- `size`: Size of the content buffer in bytes
## Dependencies
- Functions called/Symbols referenced:
  - [pg_fatal](../p/pg_fatal.md)
  - memcpy
  - IsValidWalSegSize
  - ngettext
  - pg_log_error
  - pg_log_error_detail
  - [checkControlFile](../c/checkControlFile.md)
- Called from (representative examples):
  - [main](../m/main.md) (multiple calls in pg_rewind.c)
  - [perform_rewind](../p/perform_rewind.md)

## Notes and Other Information
- This is a static function local to pg_rewind.c
- The function will terminate the program (exit(1)) if WAL segment size validation fails
- Sets the global WalSegSz variable based on the control file data
- WAL segment size must be a power of two between 1 MB and 1 GB
- Located at src/bin/pg_rewind/pg_rewind.c:1023-1055

## Simplified Source

```c
static void digestControlFile(ControlFileData *ControlFile, const char *content, size_t size)
{
    // Verify control file size is correct
    if (size != PG_CONTROL_FILE_SIZE)
        pg_fatal("unexpected control file size %d, expected %d",
                (int) size, PG_CONTROL_FILE_SIZE);

    // Copy raw control file data to structure
    memcpy(ControlFile, content, sizeof(ControlFileData));

    // Extract and validate WAL segment size
    WalSegSz = ControlFile->xlog_seg_size;

    if (!IsValidWalSegSize(WalSegSz)) {
        pg_log_error(ngettext("invalid WAL segment size in control file (%d byte)",
                             "invalid WAL segment size in control file (%d bytes)",
                             WalSegSz),
                    WalSegSz);
        pg_log_error_detail("The WAL segment size must be a power of two between 1 MB and 1 GB.");
        exit(1);
    }

    // Perform additional control file validation
    checkControlFile(ControlFile);
}
```