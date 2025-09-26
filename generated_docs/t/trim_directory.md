# trim_directory

## Location
[src/port/path.c:1070-1101](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/path.c#L1070-L1101)

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
  - [skip_drive](../s/skip_drive.md)
  - IS_DIR_SEP (macro, used multiple times)
- Called from (representative examples):
  - [canonicalize_path_enc](../c/canonicalize_path_enc.md)
  - [make_relative_path](../m/make_relative_path.md)
  - [get_parent_directory](../g/get_parent_directory.md)

## Notes and Other Information
- This is a static function, only available within src/port/path.c
- Returns a pointer to the new end of the string after trimming
- Handles multiple consecutive slashes by removing them all
- Preserves leading slashes to maintain absolute path semantics
- Handles drive letters on Windows platforms through skip_drive()
- Used as the core implementation for higher-level path manipulation functions
- Input string must be mutable (cannot be a string literal)
- Designed to be robust across different filesystem path conventions