# skipfile

## Location
src/bin/pg_checksums/pg_checksums.c: 158 - 175

## Overview
The `skipfile` function determines whether a given file should be excluded from PostgreSQL checksum validation by checking it against a predefined exclusion list.

## Definition
```c
static bool skipfile(const char *fn)
```

## Detailed Description
This function implements a file exclusion mechanism for the pg_checksums utility, which validates data page checksums in PostgreSQL database clusters. The function iterates through a static array of exclusion patterns to determine if a file should be skipped during checksum validation.

The function supports two types of matching:
1. **Exact matching**: Files that must match the exclusion entry exactly
2. **Prefix matching**: Files that match the exclusion entry as a prefix (for excluding multiple files with common prefixes)

The exclusion list includes critical system files that either:
- Change frequently during normal operation (like `pg_control`)
- Are not data files requiring checksum validation
- Are temporary or dynamic files that should not be checked

The exclusion list is kept synchronized with the list used in basebackup.c to ensure consistency across PostgreSQL utilities.

## Parameters / Member Variables
- `fn`: The filename to check against the exclusion list

## Dependencies
- Functions called/Symbols referenced:
  - `strlen` (standard C library function for string length)
  - `strncmp` (standard C library function for string comparison)
  - [skip](skip.md) (static array of exclude_list_item structures)

- Called from (representative examples):
  - [scan_directory](scan_directory.md) function in pg_checksums.c during directory traversal

## Notes and Other Information
- This is a static function with internal linkage, accessible only within pg_checksums.c
- Returns `true` if the file should be skipped, `false` if it should be processed
- The `skip` array contains entries for system files like:
  - `pg_control`: The main control file (exact match)
  - `pg_filenode.map`: File-to-OID mapping file (exact match)  
  - `pg_internal.init`: Internal initialization files (prefix match)
  - `PG_VERSION`: Version identifier file (exact match)
  - `config_exec_params`: Configuration parameters (prefix match, EXEC_BACKEND only)
- The function handles both exact filename matches and prefix matches based on the `match_prefix` flag in the exclusion list
- When `match_prefix` is false, an extra character is added to the comparison length to ensure exact matching
- The exclusion list is terminated with a NULL entry to indicate the end of the array
- This mechanism ensures that system files that are not subject to checksum validation are properly excluded from verification processes