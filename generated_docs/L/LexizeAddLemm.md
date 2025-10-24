# LexizeAddLemm

## Location
[src/backend/tsearch/ts_parse.c:100-111](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/ts_parse.c#L100-L111)

## Overview
LexizeAddLemm creates and adds a new ParsedLex element to the work queue in LexizeData, storing a lexeme with its type and length for subsequent processing.

## Definition
```c
static void LexizeAddLemm(LexizeData *ld, int type, char *lemm, int lenlemm)
```

## Detailed Description
LexizeAddLemm is a static function that allocates and initializes a new ParsedLex structure, then adds it to the work queue (towork list) within the LexizeData structure. The function encapsulates lexeme data along with its type and length, making it available for dictionary processing and further lexical analysis.

After creating the new ParsedLex element and adding it to the tail of the work queue, the function updates the curSub pointer to reference the newly added element. This allows the lexical processing system to track the current position in the work queue and maintain processing state across multiple dictionary operations.

## Parameters / Member Variables
- `ld`: Pointer to LexizeData structure containing the work queue and processing state
- `type`: Integer representing the lexeme type (token classification)
- `lemm`: Pointer to character string containing the lexeme text
- `lenlemm`: Length of the lexeme string in characters

## Dependencies
- Functions called/Symbols referenced:
  - LexizeData (structure type)
  - [ParsedLex](../P/ParsedLex.md) (structure type)
  - [palloc](../p/palloc.md) (memory allocation function)
  - [LPLAddTail](LPLAddTail.md) (list manipulation function)
- Called from (representative examples):
  - [parsetext](../p/parsetext.md)
  - [hlparsetext](../h/hlparsetext.md)

## Notes and Other Information
- Static function with local scope to ts_parse.c
- Allocates memory using PostgreSQL's palloc for ParsedLex structure
- Updates both the work queue and current processing pointer
- Essential for building the lexeme processing pipeline
- Memory allocation follows PostgreSQL memory context patterns

## Simplified Source

```c
static void
LexizeAddLemm(LexizeData *ld, int type, char *lemm, int lenlemm)
{
    // Allocate new ParsedLex structure
    ParsedLex *newpl = palloc(sizeof(ParsedLex));

    // Initialize lexeme data
    newpl->type = type;
    newpl->lemm = lemm;
    newpl->lenlemm = lenlemm;

    // Add to work queue and update current processing pointer
    LPLAddTail(&ld->towork, newpl);
    ld->curSub = ld->towork.tail;
}
```