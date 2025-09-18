# bpchar_input

## Location
src/backend/utils/adt/varchar.c: 130 - 197

## Overview
A static utility function that serves as the common implementation for processing BPCHAR (fixed-length character) input from both text and binary sources, handling length validation, truncation, and blank-padding.

## Definition
```c
static BpChar *bpchar_input(const char *s, size_t len, int32 atttypmod, Node *escontext)
```

## Detailed Description
This function implements the core logic for processing BPCHAR input data. It handles the SQL standard requirement that CHAR(n) types are fixed-length and blank-padded. The function performs multi-byte character length validation, truncates excess characters (but only if they are spaces), and pads short strings with spaces to reach the required length. It supports both error throwing and soft error handling through the escontext parameter. The function carefully distinguishes between byte length and character length, which is crucial for proper multi-byte character set support.

## Parameters / Member Variables
- `s`: Input string data (may not be null-terminated)
- `len`: Byte length of the input string
- `atttypmod`: Type modifier specifying the target character length (includes VARHDRSZ offset)
- `escontext`: Error context for soft error handling (NULL for normal error throwing)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_mbstrlen_with_len](../p/pg_mbstrlen_with_len.md) (for multi-byte character length calculation)
  - [pg_mbcharcliplen](../p/pg_mbcharcliplen.md) (for multi-byte character boundary clipping)
  - ereturn (for soft error reporting)
  - [palloc](../p/palloc.md) (for memory allocation)
  - SET_VARSIZE (for setting variable-length header)
  - VARDATA (for accessing variable-length data area)
  - VARHDRSZ (variable header size constant)
- Called from (representative examples):
  - [bpcharin](bpcharin.md)
  - [bpcharrecv](bpcharrecv.md)

## Notes and Other Information
- Implements SQL standard behavior for CHAR(n) types with blank-padding
- Handles multi-byte character sets correctly by distinguishing byte vs character lengths
- Truncates input only if excess characters are spaces (per SQL standard)
- Returns NULL and sets error context on validation failure when escontext is provided
- The typmod parameter is measured in characters, not bytes
- Allocates result using palloc, so it's freed automatically at end of transaction
- This is a static function, only accessible within varchar.c