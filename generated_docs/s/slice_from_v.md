# slice_from_v

## Location
src/backend/snowball/libstemmer/utilities.c: 427 - 430

## Overview
Replaces the current slice in a Snowball environment with symbols from a variable-length buffer, providing a convenient interface for slice replacement with dynamically sized content.

## Definition


## Detailed Description
The `slice_from_v` function is a convenience wrapper that replaces the current slice (between bra and ket positions) in PostgreSQL's Snowball stemming environment with symbols from a variable-length buffer. It automatically determines the size of the replacement content using the SIZE macro on the provided buffer and delegates to `slice_from_s` for the actual replacement operation. This function simplifies slice replacement when working with pre-allocated symbol buffers that contain their own size information.

## Parameters / Member Variables
- `z`: Pointer to the Snowball environment structure containing the string buffer and slice boundaries  
- `p`: Pointer to a symbol buffer containing the replacement symbols (buffer includes size information)

## Dependencies
- Functions called/Symbols referenced:
  - [slice_from_s](slice_from_s.md)
  - SIZE (macro for getting buffer size)
  - symbol (type definition)
- Called from (representative examples):
  - [among](../a/among.md)

## Notes and Other Information
- This is an external function accessible from other modules in the Snowball stemmer
- Returns 0 on success, -1 on error (delegates return value from slice_from_s)
- Automatically extracts the size from the provided symbol buffer using the SIZE macro
- Provides a more convenient interface than slice_from_s when working with sized buffers
- Used in pattern matching operations like the `among` function
- Part of the public API for Snowball stemming operations in PostgreSQL