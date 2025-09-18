# checkcondition_HL

## Location
src/backend/tsearch/wparser_def.c: 1981 - 2031

## Overview
A TS_execute callback function for matching tsquery operands to headline words during PostgreSQL text search highlighting operations.

## Definition
```c
static TSTernaryValue checkcondition_HL(void *opaque, QueryOperand *val, ExecPhraseData *data)
```

## Detailed Description
The `checkcondition_HL` function serves as a callback for the TS_execute framework, specifically designed to match query operands against words in a headline context. It scans through an array of headline words to find matches with the given query operand. When matches are found, it can optionally collect position information for phrase matching and highlighting purposes. The function is careful to preserve lexeme distances rather than token distances to ensure accurate phrase matching.

## Parameters / Member Variables
- `opaque`: Pointer to an hlCheck structure containing the words array and metadata
- `val`: QueryOperand to match against the headline words
- `data`: ExecPhraseData structure for collecting position information (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - QueryOperand (query operand type)
  - ExecPhraseData (execution phrase data structure)
  - hlCheck (headline check structure type cast)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation)
  - WordEntryPos (word entry position type)
  - TS_YES/TS_NO (ternary value constants)
- Called from (representative examples):
  - [hlCover](../h/hlCover.md) (src/backend/tsearch/wparser_def.c:2157)
  - [prsd_headline](../p/prsd_headline.md) (src/backend/tsearch/wparser_def.c:2697)

## Notes and Other Information
- This function is part of PostgreSQL's text search highlighting system
- It deliberately reports lexeme positions rather than token indexes to maintain accurate phrase matching
- Allocates memory for position data only when needed and when data is not NULL
- Positions are stored in ascending order to optimize subsequent processing
- Returns TS_YES when matches are found, TS_NO otherwise
- Located in src/backend/tsearch/wparser_def.c:1981-2031