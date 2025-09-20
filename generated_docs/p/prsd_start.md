# prsd_start

## Location
[src/backend/tsearch/wparser_def.c:1896-1901](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/wparser_def.c#L1896-L1901)

## Overview
A PostgreSQL function that initializes the default text search parser with a given input string and length, serving as the start interface for text parsing operations.

## Definition

```c
Datum
prsd_start(PG_FUNCTION_ARGS)
```
## Detailed Description
prsd_start is a PostgreSQL built-in function that serves as the initialization interface for the default word parser. It acts as a wrapper around the internal TParserInit function, converting PostgreSQL function arguments into the appropriate format for parser initialization.

The function performs these operations:
1. Extracts the input text string from the first function argument using PG_GETARG_POINTER(0)
2. Extracts the text length from the second function argument using PG_GETARG_INT32(1)  
3. Calls TParserInit to create and initialize a new TParser instance
4. Returns the initialized parser as a PostgreSQL Datum pointer

TParserInit (which this function calls) handles:
- Setting up character encoding parameters
- Converting to wide character representation when needed for multibyte encodings
- Initializing the parser state machine to the base state (TPS_Base)
- Allocating necessary memory structures for parsing operations

This function is part of PostgreSQL's text search parser interface and is typically called when beginning tokenization of text for full-text search indexing or query processing.

## Parameters / Member Variables
- Argument 0: Pointer to input text string to be parsed
- Argument 1: Length of the input string in bytes
- Returns: Pointer to initialized TParser structure

## Dependencies
- Functions called/Symbols referenced:
  - TParserInit (internal parser initialization function)
  - PG_GETARG_POINTER (PostgreSQL argument extraction macro)
  - PG_GETARG_INT32 (PostgreSQL argument extraction macro)
  - PG_RETURN_POINTER (PostgreSQL return value macro)
- Called from:
  - PostgreSQL function call interface (no direct code references found)

## Notes and Other Information
- This is a PostgreSQL interface function exposed to the text search framework for parser initialization
- The function is very lightweight, serving primarily as a bridge between PostgreSQL's function calling convention and the internal parser implementation
- Memory management for the returned TParser structure follows PostgreSQL's memory context system
- Used as part of the standard text search parser workflow: prsd_start → prsd_nexttoken (repeatedly) → cleanup