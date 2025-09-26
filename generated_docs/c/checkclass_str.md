# checkclass_str

## Location
[src/backend/utils/adt/tsvector_op.c:1189-1294](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_op.c#L1189-L1294)

## Overview
Checks if a word entry matches a query operand by examining weight restrictions and optionally collecting position information for phrase matching operations in text search queries.

## Definition

```c
static TSTernaryValue
checkclass_str(CHKVAL *chkval, WordEntry *entry, QueryOperand *val,
			   ExecPhraseData *data)
```
## Detailed Description
The  function is a core component of PostgreSQL's text search execution engine that determines whether a lexeme (word entry) in a tsvector matches the criteria specified in a query operand. It handles weight filtering (A, B, C, D weights) and optionally collects positional information needed for phrase matching.

The function returns a ternary value indicating the match result: definite match (TS_YES), definite non-match (TS_NO), or possible match requiring further evaluation (TS_MAYBE). When positional data is requested and the lexeme lacks position information, it returns TS_MAYBE to indicate uncertainty.

The implementation efficiently filters positions by weight when weight restrictions are specified, and can either just check for matches or collect matching positions for subsequent phrase processing.

## Parameters / Member Variables
- **chkval**: Structure containing tsvector data access information
  - : Pointer to beginning of WordEntry array
  - : Pointer to end of WordEntry array  
  - : Pointer to lexeme string data
  - : Pointer to operand string
- **entry**: WordEntry being evaluated for matching
- **val**: Query operand specifying match criteria including weight restrictions
- **data**: Optional structure for collecting position information (NULL if not needed)

## Dependencies
- Functions called/Symbols referenced:
  - SHORTALIGN: Align memory addresses to short boundaries
  - WEP_GETWEIGHT: Extract weight value from WordEntryPos
  - WEP_GETPOS: Extract position value from WordEntryPos
  - [palloc](../p/palloc.md): Allocate memory in PostgreSQL memory context
  - [pfree](../p/pfree.md): Free allocated memory
  - [WordEntryPosVector](../W/WordEntryPosVector.md): Structure containing position array
  - WordEntryPos: Position and weight information for lexeme
  - TSTernaryValue constants (TS_YES, TS_NO, TS_MAYBE)

- Called from (representative examples):
  - [checkcondition_str](checkcondition_str.md): Main condition checking function for text search execution

## Notes and Other Information
- Returns TS_YES for definite match, TS_NO for definite non-match, TS_MAYBE for uncertain cases
- When entry lacks positional information (), weight restrictions are ignored for compatibility
- Stripped tsvectors (without positions) are considered to match weight-restricted queries for historical reasons
- Memory allocation for position data uses  flag to track ownership
- Position filtering creates a new array containing only positions with matching weights
- Weight matching uses bitmask operations where each weight (A,B,C,D) corresponds to a bit position
- Function is optimized for common cases: simple existence checking vs. full position collection
- Critical component in text search phrase matching and ranking calculations
- Handles edge cases like empty position arrays and memory cleanup on partial matches