# should_allow_existing_directory

## Location
[src/bin/pg_basebackup/bbstreamer_file.c:292-316](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_file.c#L292-L316)

## Overview
This function determines whether an existing directory should be tolerated during archive extraction, specifically handling cases where PostgreSQL system directories may legitimately exist before extraction.

## Definition

```c
static bool
should_allow_existing_directory(const char *pathname)
```
## Detailed Description
The  function implements logic to determine when directory creation failures should be ignored during backup restoration. This is particularly important in PostgreSQL backup scenarios where certain system directories may already exist due to WAL streaming processes or in-place tablespace configurations.

The function handles two main categories of acceptable existing directories:
1. **System directories**: Core PostgreSQL directories like ,  (pre-9.6), , , and 
2. **Tablespace directories**: Numeric directories within  that represent tablespace OIDs

For WAL-related directories, the function accounts for the fact that WAL receiver processes may create these directories before backup completion, and symbolic links may be established for custom WAL directory locations. For tablespaces, it recognizes that in-place tablespace configurations may result in pre-existing directory structures.

## Parameters / Member Variables
- : Full path to the directory being checked for acceptable pre-existence

## Dependencies
- Functions called/Symbols referenced:
  - [last_dir_separator](../l/last_dir_separator.md)
  - strcmp
  - strspn
  - strlen  
  - strstr
- Called from (representative examples):
  - [extract_directory](../e/extract_directory.md)

## Notes and Other Information
- This is a static function specific to the bbstreamer file extraction implementation
- The function supports both current () and legacy () WAL directory names for backward compatibility
- Tablespace directory detection uses numeric validation combined with path pattern matching
- The logic specifically handles the case where numeric directory names appear directly under 
- This function is essential for preventing spurious errors during backup restoration in active PostgreSQL environments
- Located in src/bin/pg_basebackup/bbstreamer_file.c:292-316