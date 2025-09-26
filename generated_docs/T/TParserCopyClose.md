# TParserCopyClose

## Location
[src/backend/tsearch/wparser_def.c:397-423](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/wparser_def.c#L397-L423)

## Overview
Cleans up and deallocates a TParser copy created with TParserCopyInit, freeing only the parser structure and state stack while leaving shared string data intact.

## Definition
```c
static void TParserCopyClose(TParser *prs)
```

## Detailed Description
TParserCopyClose performs cleanup specific to TParser copies created with TParserCopyInit. Unlike TParserClose, this function does not deallocate the wide character string buffers (wstr and pgwstr) because these are shared with the original parser that created the copy. The function only frees the TParserPosition state stack and the main TParser structure itself.

This selective cleanup approach is essential for the copy parser design, where string data is shared between the original and copy parsers to avoid expensive string duplication. The original parser remains responsible for cleaning up the shared string data when it is closed.

## Parameters / Member Variables
- `prs`: The TParser copy structure to close and deallocate (created with TParserCopyInit)

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation)
  - [TParserPosition](TParserPosition.md) (structure type for state stack traversal)

- Called from (representative examples):
  - [p_ishost](../p/p_ishost.md) (host parsing function)
  - [p_isURLPath](../p/p_isURLPath.md) (URL path parsing function)

## Notes and Other Information
- This is a static function, only accessible within the wparser_def.c module
- Specifically designed for TParser copies created with TParserCopyInit
- Does NOT free wide character string buffers (wstr/pgwstr) since they are shared with the original parser
- Only frees the state stack and main parser structure
- Must be paired with TParserCopyInit - using TParserClose on a copy would incorrectly free shared string data
- Essential for proper memory management when using parser copies
- Includes conditional compilation for WPARSER_TRACE debugging support
- The original parser must handle cleanup of shared string data when it is closed