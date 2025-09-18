# testprs_end

## Location
src/test/modules/test_parser/test_parser.c: 99 - 107

## Overview
Clean up and deallocate parser state resources at the end of parsing operations in PostgreSQL test parser module.

## Definition
```c
Datum testprs_end(PG_FUNCTION_ARGS)
```

## Detailed Description
The testprs_end function serves as the cleanup function for the test parser module, responsible for properly deallocating the ParserState structure and its associated resources. This function is called when parsing operations are complete to ensure proper memory management within PostgreSQL's memory context system. It follows the standard PostgreSQL function interface and returns void to indicate successful cleanup.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function interface providing access to:
  - `PG_GETARG_POINTER(0)`: Pointer to ParserState structure to be deallocated

## Dependencies
- Functions called/Symbols referenced:
  - ParserState (structure type being deallocated)
  - pfree (PostgreSQL memory deallocation function)
  - PG_GETARG_POINTER, PG_RETURN_VOID (PostgreSQL function argument/return macros)
- Called from (representative examples):
  - LexDescr (referenced in test parser lexical description)

## Notes and Other Information
- Essential for proper memory management in PostgreSQL's memory context system
- Should be called after parsing operations are complete
- Pairs with testprs_start function which allocates the ParserState
- Uses pfree instead of standard free() to work within PostgreSQL's memory management
- Part of the standard parser lifecycle: start → getlexeme (multiple calls) → end