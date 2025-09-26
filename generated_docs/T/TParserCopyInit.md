# TParserCopyInit

## Location
[src/backend/tsearch/wparser_def.c:346-371](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/wparser_def.c#L346-L371)

## Overview
Creates a copy of an existing TParser that shares the same input string but starts parsing from the original parser's current position, avoiding expensive string copying for recursive parsing scenarios.

## Definition
```c
static TParser *TParserCopyInit(const TParser *orig)
```

## Detailed Description
TParserCopyInit creates a lightweight copy of an existing TParser structure without duplicating the input string data. This optimization is crucial for recursive parsing scenarios where multiple parsers might need to operate on the same string, as repeatedly copying long strings can cause significant performance degradation.

The copy parser shares the string pointers with the original parser but adjusts them to start from the original parser's current position (both byte and character positions). This allows the copy to parse from where the original parser currently is. The copy gets its own independent parser state initialized to TPS_Base, allowing it to parse independently while sharing the underlying string data.

## Parameters / Member Variables
- `orig`: The original TParser to copy from (must remain valid while copy is in use)

## Dependencies
- Functions called/Symbols referenced:
  - palloc0 (zero-initialized memory allocation)
  - newTParserPosition (create initial parser position)
  - TPS_Base (initial parser state constant)

- Called from (representative examples):
  - p_ishost (host parsing function)
  - p_isURLPath (URL path parsing function)

## Notes and Other Information
- This is a static function, only accessible within the wparser_def.c module
- Creates a memory-efficient parser copy by sharing string data instead of duplicating it
- The copy starts parsing from the original parser's current byte and character positions
- Both single-byte and wide character string pointers are adjusted appropriately
- The original parser must remain valid for the lifetime of the copy since string data is shared
- Useful for recursive parsing operations to avoid repeated string copying overhead
- Each copy gets its own independent parser state starting from TPS_Base
- Includes conditional compilation for WPARSER_TRACE debugging support