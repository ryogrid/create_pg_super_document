# tar_get_file_name

## Location
[src/bin/pg_basebackup/walmethods.c:825-836](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/walmethods.c#L825-L836)

## Overview
Constructs a complete filename by concatenating a pathname with an optional temporary suffix for TAR-based WAL file operations.

## Definition
```c
static char *tar_get_file_name(WalWriteMethod *wwmethod, const char *pathname, const char *temp_suffix)
```

## Detailed Description
This function creates a dynamically allocated filename string by combining a base pathname with an optional temporary suffix. It allocates memory for a maximum-length PostgreSQL path and uses snprintf to safely construct the final filename. The function is designed to handle temporary file naming conventions where files may need temporary suffixes during creation before being renamed to their final names. The resulting string must be freed by the caller.

## Parameters / Member Variables
- `wwmethod`: Pointer to WalWriteMethod structure (context parameter, not directly used in current implementation)
- `pathname`: Base pathname string for the file
- `temp_suffix`: Optional suffix string to append; if NULL, no suffix is added

## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc0](../p/pg_malloc0.md) (PostgreSQL memory allocation function)
  - snprintf (formatted string printing)
  - MAXPGPATH (maximum path length constant)
  - [WalWriteMethod](../W/WalWriteMethod.md) (method structure type)
- Called from:
  - [CreateWalDirectoryMethod](../C/CreateWalDirectoryMethod.md) (as function pointer assignment)
  - [tar_open_for_write](tar_open_for_write.md)

## Notes and Other Information
- Returns a newly allocated string that must be freed by the caller
- Uses pg_malloc0 to ensure zero-initialized memory allocation
- Handles NULL temp_suffix gracefully by treating it as an empty string
- Limited to MAXPGPATH characters for the complete filename
- Part of the WAL method interface providing filename construction abstraction

## Simplified Source

```c
static char *
tar_get_file_name(WalWriteMethod *wwmethod, const char *pathname,
                  const char *temp_suffix) {
    char *filename = pg_malloc0(MAXPGPATH * sizeof(char));

    // Construct filename by concatenating pathname and optional suffix
    snprintf(filename, MAXPGPATH, "%s%s",
             pathname, temp_suffix ? temp_suffix : "");

    return filename;
}
```