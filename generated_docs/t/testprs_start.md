# testprs_start

## Location
src/test/modules/test_parser/test_parser.c: 47 - 58

## Overview
Initialize a parser state for parsing text input in PostgreSQL test parser module.

## Definition


## Detailed Description
The testprs_start function is part of PostgreSQL's test parser module, designed to initialize and set up a parser state for text parsing operations. It creates and initializes a ParserState structure with the input text buffer and its length, setting the initial parsing position to 0. The function follows PostgreSQL's standard function interface pattern using PG_FUNCTION_ARGS and returns a Datum containing a pointer to the allocated parser state.

## Parameters / Member Variables
- : Standard PostgreSQL function interface macro that provides access to:
  - : Pointer to the text buffer to be parsed
  - : Length of the text buffer

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md) (memory allocation function)
  - [ParserState](../P/ParserState.md) (structure type for maintaining parser state)
  - PG_GETARG_POINTER, PG_GETARG_INT32, PG_RETURN_POINTER (PostgreSQL function argument macros)
- Called from (representative examples):
  - LexDescr (referenced in test parser lexical description)

## Notes and Other Information
- This function is part of PostgreSQL's test infrastructure for parser functionality
- The allocated ParserState structure contains buffer pointer, length, and current position
- Uses palloc0 for zero-initialized memory allocation within PostgreSQL's memory context system
- Returns a pointer to the initialized parser state for use by subsequent parsing functions