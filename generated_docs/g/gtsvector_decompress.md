# gtsvector_decompress

## Location
[src/backend/utils/adt/tsgistidx.c:252-278](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsgistidx.c#L252-L278)

## Overview
Decompresses a TSVector GiST index entry by handling TOAST detoasting to ensure other support functions can work with the uncompressed data.

## Definition
```c
Datum gtsvector_decompress(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is part of the GiST support infrastructure for TSVector indexing. Its primary purpose is to handle the decompression of potentially TOAST-compressed SignTSVector entries. PostgreSQL uses TOAST (The Oversized-Attribute Storage Technique) to store large values out-of-line, and this function ensures that when a TSVector index entry is accessed, it is properly detoasted so that other GiST support functions can operate on the actual data rather than TOAST pointers.

The function performs a simple but critical operation: it checks if the entry key is toasted, and if so, creates a new GISTENTRY with the detoasted data. If the entry is already uncompressed, it simply returns the original entry unchanged.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `entry`: GISTENTRY pointer containing the potentially toasted SignTSVector data

## Dependencies
- Functions called/Symbols referenced:
  - `PG_DETOAST_DATUM`: PostgreSQL macro to decompress TOAST-compressed data
  - `gistentryinit`: Initializes a new GiST entry structure
  - [SignTSVector](../S/SignTSVector.md): TSVector signature structure type
  - [GISTENTRY](../G/GISTENTRY.md): GiST entry structure type
- Called from:
  - GiST index operations (no direct references found in current analysis)

## Notes and Other Information
- Essential for proper functioning of TSVector GiST indexes with large entries
- Works in conjunction with PostgreSQL TOAST system for managing oversized attributes
- Creates a new GISTENTRY only when detoasting is actually needed, avoiding unnecessary memory allocation
- Part of the standard GiST operator class interface for TSVector data types
- The comment emphasizes that other gtsvector support functions cannot handle toasted values, making this function crucial for the index infrastructure