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

## Simplified Source

```c
static void
tablespace_list_append(const char *arg)
{
    TablespaceListCell *cell = (TablespaceListCell *) pg_malloc0(sizeof(TablespaceListCell));
    char *dst_ptr = cell->old_dir;
    const char *arg_ptr;

    // Parse the argument string, looking for unescaped '=' separator
    for (arg_ptr = arg; *arg_ptr; arg_ptr++) {
        if (dst_ptr - cell->old_dir >= MAXPGPATH)
            pg_fatal("directory name too long");

        if (*arg_ptr == '\\' && *(arg_ptr + 1) == '=') {
            // Skip backslash escaping =
            continue;
        }
        else if (*arg_ptr == '=' && (arg_ptr == arg || *(arg_ptr - 1) != '\\')) {
            // Found unescaped separator - switch to new_dir
            if (*cell->new_dir)
                pg_fatal("multiple \"=\" signs in tablespace mapping");
            else
                dst_ptr = cell->new_dir;
        }
        else {
            *dst_ptr++ = *arg_ptr;
        }
    }

    // Validate that both directories were specified
    if (!*cell->old_dir || !*cell->new_dir)
        pg_fatal("invalid tablespace mapping format \"%s\", must be \"OLDDIR=NEWDIR\"", arg);

    // Validate that old directory is absolute path (Windows or Unix style)
    if (!is_nonwindows_absolute_path(cell->old_dir) && !is_windows_absolute_path(cell->old_dir))
        pg_fatal("old directory is not an absolute path in tablespace mapping: %s", cell->old_dir);

    // Validate that new directory is absolute path
    if (!is_absolute_path(cell->new_dir))
        pg_fatal("new directory is not an absolute path in tablespace mapping: %s", cell->new_dir);

    // Canonicalize paths for consistent comparisons
    canonicalize_path(cell->old_dir);
    canonicalize_path(cell->new_dir);

    // Add to linked list
    if (tablespace_dirs.tail)
        tablespace_dirs.tail->next = cell;
    else
        tablespace_dirs.head = cell;
    tablespace_dirs.tail = cell;
}
```