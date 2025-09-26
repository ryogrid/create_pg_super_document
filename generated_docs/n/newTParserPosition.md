# newTParserPosition

## Location
src/backend/tsearch/wparser_def.c: 272 - 288

## Overview
Creates and initializes a new TParserPosition structure for tracking parser state in PostgreSQL's text search word parser.

## Definition

```c
static TParserPosition *
newTParserPosition(TParserPosition *prev)
```
## Detailed Description
The newTParserPosition function allocates and initializes a new TParserPosition structure used to maintain parser state information in PostgreSQL's text search functionality. This function creates a linked list of parser positions, allowing the parser to maintain a stack of positions for backtracking or nested parsing operations.

If a previous position is provided, the function copies all fields from the previous position to maintain continuity. If no previous position is provided, the structure is zero-initialized. The function always sets up the linked list relationship by storing the previous position pointer and initializes the pushedAtAction field to NULL.

## Parameters / Member Variables
- `prev`: Previous TParserPosition in the linked list chain, or NULL if this is the first/root position

## Dependencies
- Functions called/Symbols referenced:
  - palloc (memory allocation)
  - memcpy (copying previous state)
  - memset (zero initialization)
  - TParserPosition (structure type)

- Called from (representative examples):
  - TParserInit
  - TParserCopyInit
  - p_isURLPath
  - TParserGet

## Notes and Other Information
- This is a static function, only accessible within the wparser_def.c module
- Creates a linked list structure allowing parser state stacking
- Memory is allocated using PostgreSQL's palloc which is automatically freed at end of memory context
- The pushedAtAction field is always initialized to NULL regardless of previous state
- Used internally by the text search word parser for position tracking and state management