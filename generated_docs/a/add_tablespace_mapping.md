# add_tablespace_mapping

## Location
src/bin/pg_combinebackup/pg_combinebackup.c: 436 - 500

## Overview
Processes the argument for the -T, --tablespace-mapping switch in pg_combinebackup, parsing and validating tablespace directory mappings from command line input.

## Definition
```c
static void add_tablespace_mapping(cb_options *opt, char *arg)
```

## Detailed Description
The add_tablespace_mapping function processes tablespace mapping arguments provided via the -T or --tablespace-mapping command line option in pg_combinebackup. It parses the input string in "OLDDIR=NEWDIR" format and creates a cb_tablespace_mapping structure to store the mapping information.

The function handles several important aspects:
1. **String parsing**: Splits the argument on the equals sign while properly handling escaped equals signs (\\=)
2. **Validation**: Ensures both old and new directories are specified and are absolute paths
3. **Path canonicalization**: Normalizes paths to avoid spurious comparison failures
4. **Error handling**: Provides detailed error messages for malformed input
5. **List management**: Adds the new mapping to the linked list of tablespace mappings

The function is designed to handle backslash-escaped equals signs, allowing directory names containing literal equals signs to be specified correctly.

## Parameters / Member Variables
- `opt`: Pointer to cb_options structure containing command-line options and tablespace mappings list
- `arg`: Command-line argument string in "OLDDIR=NEWDIR" format to be parsed

## Dependencies
- Functions called/Symbols referenced:
  - cb_options (options structure type)
  - cb_tablespace_mapping (tablespace mapping structure type)
  - pg_malloc0 (zero-initialized memory allocation)
  - is_absolute_path (path validation utility)
  - canonicalize_path (path normalization utility)
- Called from (representative examples):
  - main (command-line argument processing)

## Notes and Other Information
- Located in src/bin/pg_combinebackup/pg_combinebackup.c:436-500
- Unlike pg_basebackup, both old and new directories are on the local machine
- Requires absolute paths for both source and destination directories since tablespaces are always created with absolute paths
- Maintains a linked list of tablespace mappings in the options structure
- Handles backslash escaping to allow literal equals signs in directory names
- Performs path canonicalization to ensure consistent path comparison later in the process