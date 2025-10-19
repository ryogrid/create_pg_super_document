# dir_existsfile

## Location
[src/bin/pg_basebackup/walmethods.c:584-607](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/walmethods.c#L584-L607)

## Overview
Checks whether a file exists within a directory-based WAL writing method implementation.

## Definition
```c
static bool dir_existsfile(WalWriteMethod *wwmethod, const char *pathname)
```

## Detailed Description
This function is a static implementation of the file existence check operation for the directory-based WAL writing method. It constructs the full file path by combining the base directory from the DirectoryMethodData structure with the provided pathname, then attempts to open the file in read-only mode to determine if it exists. The function uses a simple approach of trying to open the file and immediately closing it if successful. It deliberately does not set error information when the file doesn't exist, as this is expected behavior for an existence check.

## Parameters / Member Variables
- `wwmethod`: Pointer to the WalWriteMethod structure containing the directory method data
- `pathname`: Relative path of the file whose existence is to be checked

## Dependencies
- Functions called/Symbols referenced:
  - clear_error (internal function)
  - snprintf (system function)  
  - open (system function)
  - close (system function)
  - PG_BINARY (PostgreSQL macro)
- Data structures used:
  - [WalWriteMethod](../W/WalWriteMethod.md)
  - [DirectoryMethodData](../D/DirectoryMethodData.md)
- Called from:
  - Used as a function pointer in WAL writing method operations

## Notes and Other Information
- Returns true if the file exists and can be opened, false otherwise
- Does not set lasterrno when file doesn't exist, as this is not considered an error condition
- Calls clear_error() at the beginning to reset any previous error state
- Opens file with O_RDONLY | PG_BINARY flags for cross-platform compatibility
- Part of the directory-based WAL writing method implementation for pg_basebackup
- Static function, only accessible within the walmethods.c compilation unit

## Simplified Source

```c
static bool
dir_existsfile(WalWriteMethod *wwmethod, const char *pathname)
{
    DirectoryMethodData *dir_data = (DirectoryMethodData *) wwmethod;
    char tmppath[MAXPGPATH];
    int fd;

    clear_error(wwmethod);

    // Build full path: basedir + pathname
    snprintf(tmppath, sizeof(tmppath), "%s/%s", dir_data->basedir, pathname);

    // Try to open file - if it opens, it exists
    fd = open(tmppath, O_RDONLY | PG_BINARY, 0);
    if (fd < 0)
        return false;

    close(fd);
    return true;
}
```