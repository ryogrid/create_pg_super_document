# extract_directory

## Location
[src/bin/pg_basebackup/bbstreamer_file.c:317-341](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_file.c#L317-L341)

## Overview
This function creates directories during archive extraction, handling both the directory creation process and permission setting while gracefully managing cases where directories may already exist.

## Definition

```c
static void
extract_directory(const char *filename, mode_t mode)
```
## Detailed Description
The  function is responsible for creating directories as part of the archive extraction process in pg_basebackup. It performs directory creation with appropriate error handling and permission management.

The function first attempts to create the directory using  with . If the creation fails, it checks whether the failure is due to an already existing directory. In such cases, it consults  to determine if the existing directory should be tolerated (such as for PostgreSQL system directories that may have been created by other processes).

On non-Windows systems, the function also sets the directory permissions to match those specified in the archive using . This ensures that extracted directories maintain their original permission settings from the backup.

## Parameters / Member Variables
- `*filename`: Path where the directory should be created
- `mode`: File permissions that should be applied to the created directory
## Dependencies
- Functions called/Symbols referenced:
  - mkdir
  - [should_allow_existing_directory](../s/should_allow_existing_directory.md)
  - chmod (non-Windows only)
  - [pg_fatal](../p/pg_fatal.md)
  - pg_dir_create_mode (global variable)
- Called from (representative examples):
  - [bbstreamer_extractor_content](../b/bbstreamer_extractor_content.md)

## Notes and Other Information
- This is a static function specific to the bbstreamer file extraction implementation
- Permission setting via  is skipped on Windows platforms due to different permission models
- The function uses  for initial directory creation, then applies archive-specific permissions
- Error handling distinguishes between creation failures due to existing directories versus other system errors
- The function integrates with PostgreSQL's error reporting system through
- Located in src/bin/pg_basebackup/bbstreamer_file.c:317-341

## Simplified Source

```c
static void
extract_directory(const char *filename, mode_t mode)
{
    // Create directory, allow existing if it's a system directory
    if (mkdir(filename, pg_dir_create_mode) != 0 &&
        (errno != EEXIST || !should_allow_existing_directory(filename)))
        pg_fatal("could not create directory \"%s\": %m", filename);

#ifndef WIN32
    // Set correct permissions on non-Windows systems
    if (chmod(filename, mode))
        pg_fatal("could not set permissions on directory \"%s\": %m", filename);
#endif
}
```