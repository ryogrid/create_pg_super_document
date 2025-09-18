# dir_get_file_name

## Location
src/bin/pg_basebackup/walmethods.c: 102 - 116

## Overview
Constructs a filename for WAL files based on the given pathname, compression algorithm, and optional temporary suffix.

## Definition
```c
static char *
dir_get_file_name(WalWriteMethod *wwmethod,
                  const char *pathname, const char *temp_suffix)
```

## Detailed Description
This function is a helper function in the pg_basebackup directory-based WAL writing method. It constructs the final filename for WAL files by combining the base pathname with appropriate file extensions based on the compression algorithm being used and optionally appending a temporary suffix. The function allocates memory for the resulting filename and formats it according to the compression settings.

The function supports different compression algorithms:
- GZIP compression: adds ".gz" extension
- LZ4 compression: adds ".lz4" extension  
- No compression: no additional extension

## Parameters / Member Variables
- `wwmethod`: Pointer to WalWriteMethod structure containing compression algorithm configuration
- `pathname`: Base pathname for the WAL file
- `temp_suffix`: Optional temporary suffix to append to the filename (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - pg_malloc0 (memory allocation)
  - snprintf (string formatting)
  - PG_COMPRESSION_GZIP (compression constant)
  - PG_COMPRESSION_LZ4 (compression constant)
- Called from (representative examples):
  - [dir_open_for_write](dir_open_for_write.md)
  - [dir_close](dir_close.md)

## Notes and Other Information
- This is a static function, only accessible within the walmethods.c file
- The function allocates MAXPGPATH bytes for the filename buffer
- Memory allocated by this function must be freed by the caller
- Part of the directory-based WAL writing method implementation in pg_basebackup