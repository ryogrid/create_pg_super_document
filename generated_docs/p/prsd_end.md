# prsd_end

## Location
[src/backend/tsearch/wparser_def.c:1918-1931](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/wparser_def.c#L1918-L1931)

## Overview
PostgreSQL text search parser function that finalizes and cleans up a parser session by closing the TParser instance.

## Definition


## Detailed Description
The  function serves as the cleanup function for PostgreSQL's default text search parser. It is responsible for properly closing and deallocating resources associated with a TParser instance that was previously created and used for text parsing operations. This function is part of the parser interface that allows PostgreSQL to tokenize and parse text for full-text search functionality.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function arguments
  - Argument 0: Pointer to a TParser instance that needs to be closed

## Dependencies
- Functions called/Symbols referenced:
  - [TParser](../T/TParser.md) (type cast for parser instance)
  - TParserClose (closes and cleans up the parser)
  - PG_RETURN_VOID (PostgreSQL macro for returning void from a function)
- Called from (representative examples):
  - No direct references found in the codebase (likely called through parser function table)

## Notes and Other Information
- This function is part of PostgreSQL's extensible text search parser framework
- It follows the PostgreSQL function calling convention using PG_FUNCTION_ARGS
- The function properly handles resource cleanup to prevent memory leaks
- Located in src/backend/tsearch/wparser_def.c:1918-1931
- Works in conjunction with other parser functions like prsd_start and prsd_nexttoken to provide complete parsing functionality