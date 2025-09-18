# get_tablespace_mapping

## Location
src/bin/pg_basebackup/pg_basebackup.c: 1678 - 1697

## Overview
get_tablespace_mapping is a utility function in pg_basebackup that retrieves the appropriate tablespace path, returning either the original path or a user-specified relocated path based on tablespace mapping configuration.

## Definition
```c
static const char *get_tablespace_mapping(const char *dir)
```

## Detailed Description
This function implements tablespace path mapping functionality for the pg_basebackup utility. It allows users to relocate tablespaces during backup operations using the -T command-line option. When called, the function searches through a linked list of tablespace mappings to find if the provided directory path has been remapped to a new location.

The function first canonicalizes the input path to ensure consistent comparison, handling differences in path representation (such as trailing slashes, relative vs absolute paths, etc.). It then iterates through the global tablespace_dirs list, comparing the canonicalized input path against the old_dir field of each mapping entry.

If a matching mapping is found, the function returns the corresponding new_dir (the relocated path). If no mapping exists for the given directory, it returns the original directory path unchanged. This behavior ensures that unmapped tablespaces maintain their original locations while mapped ones are redirected as specified by the user.

## Parameters / Member Variables
- `dir`: The original tablespace directory path to look up for potential mapping

## Dependencies
- Functions called/Symbols referenced:
  - strlcpy
  - canonicalize_path
  - TablespaceListCell (struct type)
  - tablespace_dirs (global variable)
- Called from (representative examples):
  - CreateBackupStreamer
  - BaseBackup

## Notes and Other Information
- This is a static function, only accessible within the pg_basebackup.c compilation unit
- The function relies on the global tablespace_dirs linked list which is populated by command-line parsing of -T options
- Path canonicalization ensures robust matching regardless of how paths are specified (with/without trailing slashes, relative vs absolute)
- Returns a const char* pointing either to the original input or to a string stored in the tablespace mapping list
- The function is central to pg_basebackup's tablespace relocation feature, allowing users to change tablespace locations during backup restore
- Used extensively throughout the backup creation process wherever tablespace paths need to be resolved