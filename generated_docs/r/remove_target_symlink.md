# remove_target_symlink

## Location
[src/bin/pg_rewind/file_ops.c:271-293](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/file_ops.c#L271-L293)

## Overview
Removes a symbolic link file from the target data directory during PostgreSQL rewind operations.

## Definition

```c
struct stat statbuf;
```
## Detailed Description
This function is part of the pg_rewind utility's file operation system. It safely removes symbolic links from the target PostgreSQL data directory by constructing the full target path and performing an unlink operation. The function includes dry-run support and proper error handling with fatal error reporting if the unlink operation fails.

## Parameters / Member Variables
- : Relative path of the symbolic link to be removed from the target data directory

## Dependencies
- Functions called/Symbols referenced:
  - unlink (system call)
  - snprintf (standard library)
  - [pg_fatal](../p/pg_fatal.md) (PostgreSQL error reporting)
- Called from (representative examples):
  - [remove_target](remove_target.md) (file_ops.c:146)

## Notes and Other Information
- This is a static function, only accessible within the file_ops.c module
- Respects the global dry_run flag - no actual removal occurs during dry-run mode
- Uses MAXPGPATH for path buffer sizing to ensure safe path construction
- Provides detailed error messages including the full path and system error details when unlink fails
- Part of the pg_rewind utility which synchronizes a PostgreSQL cluster with another timeline