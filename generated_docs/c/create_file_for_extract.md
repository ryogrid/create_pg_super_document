# create_file_for_extract

## Location
[src/bin/pg_basebackup/bbstreamer_file.c:355-377](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_file.c#L355-L377)

## Overview
This function creates regular files during archive extraction, opening them for writing and setting appropriate permissions while returning a file handle for subsequent content writing.

## Definition

```c
static FILE *
create_file_for_extract(const char *filename, mode_t mode)
```
## Detailed Description
The  function is responsible for creating regular files as part of the archive extraction process in pg_basebackup. It serves as a comprehensive file creation utility that handles both file opening and permission management.

The function opens the specified file in binary write mode ("wb") using , which creates a new file or truncates an existing file. After successful creation, on non-Windows platforms, it applies the specified permissions using  to match the original file permissions stored in the archive.

The function returns a FILE pointer that can be used by the calling code to write the file contents. This design allows for efficient streaming of file data during extraction, as the file handle remains open for subsequent write operations.

## Parameters / Member Variables
- `*filename`: Path where the file should be created
- `mode`: File permissions that should be applied to the created file
## Dependencies
- Functions called/Symbols referenced:
  - fopen
  - chmod (non-Windows only)
  - [pg_fatal](../p/pg_fatal.md)
- Called from (representative examples):
  - [bbstreamer_extractor_content](../b/bbstreamer_extractor_content.md)

## Notes and Other Information
- This is a static function specific to the bbstreamer file extraction implementation  
- Returns a FILE pointer for subsequent writing operations by the caller
- Permission setting via  is skipped on Windows platforms due to different permission models
- Uses binary write mode ("wb") for proper handling of all file types including binary files
- Error handling provides clear diagnostic messages through PostgreSQL's  mechanism
- The function creates new files or truncates existing files with the same name
- Located in src/bin/pg_basebackup/bbstreamer_file.c:355-377

## Simplified Source

```c
static FILE *
create_file_for_extract(const char *filename, mode_t mode)
{
    // Open file for binary writing
    FILE *file = fopen(filename, "wb");
    if (file == NULL)
        pg_fatal("could not create file \"%s\": %m", filename);

    // Set file permissions (except on Windows)
#ifndef WIN32
    if (chmod(filename, mode))
        pg_fatal("could not set permissions on file \"%s\": %m", filename);
#endif

    return file;
}
```