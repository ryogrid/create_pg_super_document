# tablespace_list_append

## Location
[src/bin/pg_basebackup/pg_basebackup.c:320-389](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_basebackup.c#L320-L389)

## Overview
Parses and validates tablespace mapping arguments and appends them to the global tablespace mapping list for pg_basebackup operations.

## Definition

```c
static void
tablespace_list_append(const char *arg)
```
## Detailed Description
This function processes command-line arguments that specify tablespace directory mappings for pg_basebackup. It parses the input string in the format "OLDDIR=NEWDIR", validates both paths, and adds the mapping to a linked list for use during backup operations.

The function performs several important validations:
- Ensures the input format is correct (contains exactly one unescaped '=' sign)
- Verifies that both old and new directories are specified
- Checks that the old directory path is absolute (using either Windows or Unix path conventions)
- Verifies that the new directory path is absolute
- Canonicalizes both paths to ensure consistent comparisons

The parser handles escaped equals signs (\=) in directory names, allowing for paths that contain literal equals signs.

## Parameters / Member Variables
- `*arg`: Input string in format "OLDDIR=NEWDIR" specifying the tablespace mapping
## Dependencies
- Functions called/Symbols referenced:
  - [TablespaceListCell](../T/TablespaceListCell.md) (struct type for storing mappings)
  - [pg_malloc0](../p/pg_malloc0.md) (PostgreSQL memory allocation function)
  - is_nonwindows_absolute_path (path validation for Unix-style absolute paths)
  - is_windows_absolute_path (path validation for Windows-style absolute paths)
  - is_absolute_path (general absolute path validation)
  - [canonicalize_path](../c/canonicalize_path.md) (path normalization function)
- Called from (representative examples):
  - CompressionLocation (in pg_basebackup.c)
  - [main](../m/main.md) (in pg_basebackup.c for processing -T option arguments)

## Notes and Other Information
- This is a static function with internal linkage within pg_basebackup.c
- Used to process the -T (--tablespace-mapping) command-line option in pg_basebackup
- The function builds a linked list of tablespace mappings stored in the global tablespace_dirs structure
- [Path](../P/Path.md) validation accepts either Windows or Unix absolute path formats to handle cross-platform scenarios
- Escaped equals signs (\=) are supported in directory names for edge cases
- Both old and new directory paths are canonicalized to ensure consistent string comparisons during backup
- The old directory path is validated against both Windows and Unix absolute path rules since the source database might be on a different platform than pg_basebackup
- Fatal errors are raised for malformed input rather than returning error codes, following PostgreSQL client utility conventions