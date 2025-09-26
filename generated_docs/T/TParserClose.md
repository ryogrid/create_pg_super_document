# TParserClose

## Location
[src/backend/tsearch/wparser_def.c:372-396](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/wparser_def.c#L372-L396)

## Overview
Properly cleans up and deallocates all memory associated with a TParser structure, including its state stack and wide character string buffers.

## Definition
```c
static void TParserClose(TParser *prs)
```

## Detailed Description
TParserClose performs complete cleanup of a TParser structure by deallocating all associated memory. The function systematically traverses and frees the entire linked list of TParserPosition structures that represent the parser's state stack, ensuring no memory leaks occur from nested or stacked parser states.

After cleaning up the state stack, the function deallocates any wide character string buffers (both wstr for general locales and pgwstr for C locale) that were allocated during parser initialization. Finally, it frees the main TParser structure itself. This thorough cleanup is essential for preventing memory leaks in PostgreSQL's text search functionality.

## Parameters / Member Variables
- `prs`: The TParser structure to close and deallocate

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation)
  - [TParserPosition](TParserPosition.md) (structure type for state stack traversal)

- Called from (representative examples):
  - [prsd_end](../p/prsd_end.md) (text search parser end function)

## Notes and Other Information
- This is a static function, only accessible within the wparser_def.c module
- Systematically traverses and frees the entire TParserPosition state stack to prevent memory leaks
- Handles cleanup of both types of wide character string buffers (wstr and pgwstr)
- Essential for proper memory management in PostgreSQL's memory context system
- Must be called for every TParser created with TParserInit to avoid memory leaks
- Uses pfree consistently with PostgreSQL's memory management conventions
- Includes conditional compilation for WPARSER_TRACE debugging support
- Safe to call even if wide character buffers were not allocated (checks for NULL pointers)