# tsvectorin

## Location
src/backend/utils/adt/tsvector.c: 175 - 313

## Overview
PostgreSQL input function that parses a string representation of a tsvector and converts it into the internal TSVector data structure.

## Definition
```c
Datum tsvectorin(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the primary input conversion routine for tsvector data type in PostgreSQL. It parses a text string containing words and optional positional information, validates the input against various limits, and constructs the internal TSVector representation. The function handles dynamic memory allocation for both the word entries array and the temporary string buffer, expanding them as needed during parsing. It processes each token through the tsvector parser, collecting words and their positions, then uses uniqueentry to eliminate duplicates and merge position information. Finally, it constructs the compact TSVector structure with proper alignment and memory layout required for efficient storage and retrieval.

## Parameters / Member Variables
- Function follows PostgreSQL's V1 calling convention, receiving arguments through PG_FUNCTION_ARGS macro:
  - Input string (accessed via PG_GETARG_CSTRING(0))
  - Error context for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  - [init_tsvector_parser](../i/init_tsvector_parser.md) (initialize parser state)
  - [gettoken_tsvector](../g/gettoken_tsvector.md) (extract tokens from input)
  - [close_tsvector_parser](../c/close_tsvector_parser.md) (cleanup parser resources)
  - [uniqueentry](../u/uniqueentry.md) (remove duplicates and merge positions)
  - [palloc](../p/palloc.md)/palloc0 (PostgreSQL memory allocation)
  - [repalloc](../r/repalloc.md) (PostgreSQL memory reallocation)
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation)
  - ereturn (error return with context)
  - SOFT_ERROR_OCCURRED (check for parsing errors)
  - CALCDATASIZE (calculate total TSVector size)
  - SET_VARSIZE (set PostgreSQL variable-length header)
  - ARRPTR/STRPTR (access TSVector components)
  - SHORTALIGN (ensure proper memory alignment)
  - PG_RETURN_TSVECTOR (return TSVector result)
- Called from:
  - PostgreSQL type system (no direct references found in symbol analysis)

## Notes and Other Information
- Enforces multiple limits: MAXSTRLEN for individual words, MAXSTRPOS for total string length, MAXNUMPOS for position arrays
- Uses dynamic buffer expansion strategy starting with 256 bytes for temporary storage and 64 entries for word array
- Implements comprehensive error handling with soft error reporting through escontext
- Constructs memory-efficient TSVector layout with string data and positional information properly aligned
- Critical entry point for converting text input into PostgreSQL's full-text search data structures
- The function maintains referential integrity between word entries and their string/position data throughout the conversion process