# varchar_input

## Location
src/backend/utils/adt/varchar.c: 457 - 494

## Overview
Internal function that handles the common logic for varchar input processing, performing length validation and truncation according to SQL standards.

## Definition
```c
static VarChar *varchar_input(const char *s, size_t len, int32 atttypmod, Node *escontext)
```

## Detailed Description
The `varchar_input` function serves as the core input processing routine for PostgreSQL's varchar data type. It handles both text input (`varcharin`) and binary input (`varcharrecv`) by performing length validation and truncation according to SQL standards. The function enforces varchar length constraints by checking if the input exceeds the specified maximum length, and allows truncation only if the excess characters are spaces. It uses multibyte character-aware processing to ensure proper handling of non-ASCII characters and supports soft error handling through an error context parameter.

## Parameters / Member Variables
- `s`: Pointer to input character string (may not be null-terminated)
- `len`: Length of the input string in bytes
- `atttypmod`: Type modifier specifying the maximum allowed character length (includes header size)
- `escontext`: Error context node for soft error handling, or NULL for hard errors

## Dependencies
- Functions called/Symbols referenced:
  - `[pg_mbcharcliplen](../p/pg_mbcharcliplen.md)`: Calculates maximum byte length for multibyte character truncation
  - `ereturn`: Returns error through soft error context mechanism
  - `cstring_to_text_with_len`: Converts C string to VarChar/text with specified length
  - `[errcode](../e/errcode.md)`: Sets error code for string truncation violations
  - `[errmsg](../e/errmsg.md)`: Formats error message for length violations
- Called from (representative examples):
  - `[varcharin](varcharin.md)`: Text input function for varchar type
  - `[varcharrecv](varcharrecv.md)`: Binary receive function for varchar type

## Notes and Other Information
- Located in `src/backend/utils/adt/varchar.c:457-494`
- Static function shared by both text and binary input paths for varchar
- Implements SQL standard behavior for varchar length enforcement
- Allows truncation of trailing spaces but raises error for truncation of non-space characters
- Uses multibyte character awareness to handle international character sets correctly
- Binary-compatible with text type, allowing reuse of text conversion functions
- Supports PostgreSQL's soft error handling mechanism through error context parameter
- The `atttypmod` parameter is measured in characters, not bytes, requiring multibyte-aware processing