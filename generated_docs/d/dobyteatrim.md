# dobyteatrim

## Location
src/backend/utils/adt/oracle_compat.c: 534 - 616

## Overview
The dobyteatrim function is the core implementation that provides byte-level trimming functionality for PostgreSQL's bytea (binary data) trim functions.

## Definition
```c
bytea *dobyteatrim(bytea *string, bytea *set, bool doltrim, bool dortrim)
```

## Detailed Description
dobyteatrim is a helper function that implements the common trimming logic for binary data (bytea type) used by byteatrim, bytealtrim, and byteartrim functions. Unlike the text-based dotrim function, this function operates on raw bytes without concern for character encoding or multibyte sequences. It removes specified bytes from the left side, right side, or both sides of a bytea value based on a set of bytes to be trimmed. The function uses a simple byte-by-byte comparison algorithm.

## Parameters / Member Variables
- `string`: The input bytea value to be trimmed
- `set`: The bytea value containing the set of bytes to remove during trimming
- `doltrim`: Boolean flag to enable trimming from the left (start) of the bytea
- `dortrim`: Boolean flag to enable trimming from the right (end) of the bytea

## Dependencies
- Functions called/Symbols referenced:
  - SET_VARSIZE (set the size of a variable-length PostgreSQL data type)
  - VARDATA (get pointer to the actual data within a variable-length type)
- Called from (representative examples):
  - [byteatrim](../b/byteatrim.md) (bidirectional bytea trimming)
  - [bytealtrim](../b/bytealtrim.md) (left-side bytea trimming)
  - [byteartrim](../b/byteartrim.md) (right-side bytea trimming)

## Notes and Other Information
- Located in src/backend/utils/adt/oracle_compat.c:534-616
- Operates on raw binary data without character encoding considerations
- Uses simple byte-by-byte comparison, making it more efficient than text trimming for binary data
- Returns the original string unchanged if either string or set is empty
- Allocates new bytea structure for the result and copies the trimmed portion
- Part of PostgreSQL's Oracle compatibility layer for binary data operations