# slice_from_s

## Location
[src/backend/snowball/libstemmer/utilities.c:422-426](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/utilities.c#L422-L426)

## Overview
Replaces the current slice (between bra and ket positions) in a Snowball environment with a new sequence of symbols, providing a safe interface for slice replacement operations.

## Definition

```c
}

extern int slice_from_s(struct SN_env * z, int s_size, const symbol * s)
```
## Detailed Description
The `slice_from_s` function is a high-level wrapper for slice replacement operations in PostgreSQL's Snowball stemming environment. It first validates the slice parameters using `slice_check` to ensure the operation is safe, then delegates to `replace_s` to perform the actual replacement of symbols between the current bracket positions (`z->bra` and `z->ket`) with the provided symbol sequence. This function provides a clean interface for replacing the currently selected slice with new content while maintaining proper error handling and validation.

## Parameters / Member Variables
- `z`: Pointer to the Snowball environment structure containing the string buffer and slice boundaries
- `s_size`: Number of symbols in the replacement sequence
- `s`: Pointer to the array of replacement symbols

## Dependencies
- Functions called/Symbols referenced:
  - [slice_check](slice_check.md)
  - [replace_s](../r/replace_s.md)
  - symbol (type definition)
- Called from (representative examples):
  - No direct references found in the indexed codebase

## Notes and Other Information
- This is an external function accessible from other modules in the Snowball stemmer
- Returns 0 on success, -1 on error
- Performs validation before attempting the replacement operation
- Automatically uses the current slice boundaries (bra and ket) from the environment
- Does not provide size adjustment information (passes NULL for adjptr parameter)
- Part of the public API for Snowball stemming operations in PostgreSQL