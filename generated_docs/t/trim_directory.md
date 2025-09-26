# trim_directory

## Location
src/port/path.c: 1070 - 1101

## Overview
Removes trailing directory components from a path string, including trailing slashes and the last pathname component, while preserving leading slashes.

## Definition

```c
static char *
trim_directory(char *path)
```
## Detailed Description
This function modifies a path string in-place to remove trailing directory information. It performs several operations in sequence: removes trailing slashes, removes the last pathname component (directory or file name), and removes the slash preceding that component. However, it never removes a leading slash to preserve absolute path semantics.

The function is designed to work robustly across different platforms and handles multiple consecutive slashes appropriately. It's primarily used internally within the path manipulation system and serves as the core implementation for  and various path canonicalization operations.

For the convenience of , the function returns a pointer to the new end location of the string.

## Parameters / Member Variables
- : Input/output string containing the file path to be modified in-place (must be writable)

## Dependencies
- Functions called/Symbols referenced:
  - skip_drive
  - IS_DIR_SEP (macro, used multiple times)
- Called from (representative examples):
  - canonicalize_path_enc
  - make_relative_path
  - get_parent_directory

## Notes and Other Information
- This is a static function, only available within src/port/path.c
- Returns a pointer to the new end of the string after trimming
- Handles multiple consecutive slashes by removing them all
- Preserves leading slashes to maintain absolute path semantics
- Handles drive letters on Windows platforms through skip_drive()
- Used as the core implementation for higher-level path manipulation functions
- Input string must be mutable (cannot be a string literal)
- Designed to be robust across different filesystem path conventions