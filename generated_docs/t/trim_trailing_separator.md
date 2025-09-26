# trim_trailing_separator

## Location
[src/port/path.c:1102-1123](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/path.c#L1102-L1123)

## Overview
Removes trailing directory separator characters (slashes) from a path string while preserving any leading slash.

## Definition

```c
static void
trim_trailing_separator(char *path)
```
## Detailed Description
This function modifies a path string in-place to remove any trailing directory separator characters, but carefully preserves a leading slash to maintain absolute path semantics. It first calls  to handle Windows drive letters appropriately, then walks backward from the end of the string removing any directory separators found.

The function is designed to normalize path strings by removing superfluous trailing slashes while maintaining the essential structure of the path. This is particularly useful in path canonicalization and normalization processes where consistent path formatting is required.

## Parameters / Member Variables
- : Input/output string containing the file path to be modified in-place (must be writable)

## Dependencies
- Functions called/Symbols referenced:
  - skip_drive
  - IS_DIR_SEP (macro)
- Called from (representative examples):
  - canonicalize_path_enc
  - make_relative_path

## Notes and Other Information
- This is a static function, only available within src/port/path.c
- Void function that modifies the input string in-place
- Preserves leading slashes to maintain absolute path semantics
- Handles Windows drive letters through skip_drive()
- Used in path canonicalization and normalization operations
- Input string must be mutable (cannot be a string literal)
- Removes all trailing directory separators, not just one
- Essential component of the path manipulation toolkit in PostgreSQL