# ExecPhraseData

## Location
[src/include/tsearch/ts_utils.h:161-168](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/tsearch/ts_utils.h#L161-L168)

## Overview
ExecPhraseData is a structure passed to TSExecuteCallback functions to provide lexeme position data when processing phrase-match operators in tsquery expressions.

## Definition
```c
typedef struct ExecPhraseData
{
    int         npos;       /* number of positions reported */
    bool        allocated;  /* pos points to palloc'd data? */
    bool        negate;     /* positions are where query is NOT matched */
    WordEntryPos *pos;      /* ordered, non-duplicate lexeme positions */
    int         width;      /* width of match in lexemes, less 1 */
} ExecPhraseData;
```

## Detailed Description
ExecPhraseData is used in PostgreSQL's full-text search system to communicate position information between the tsquery execution engine and callback functions that evaluate lexeme matches. This structure is essential for phrase-match operators that require precise positional information about where lexemes occur in documents.

When a TSExecuteCallback function successfully matches lexemes (returns TS_YES), it should populate this structure with position data. If position data is unavailable, the function should leave the structure unchanged and return TS_MAYBE, indicating that a later recheck with position data will be needed.

The structure supports both positive matches (where the query matches) and negative matches (where the query does NOT match) through the negate flag. It also handles matches that span multiple lexemes through the width field. All fields are initially zeroed by the caller.

## Parameters / Member Variables
- `npos`: Number of positions reported in the pos array
- `allocated`: Boolean flag indicating whether the pos array points to palloc'd data that can be freed by the caller
- `negate`: Boolean flag indicating that positions represent where the query does NOT match rather than where it does match
- `pos`: Array of WordEntryPos values containing ordered, non-duplicate lexeme positions. Only position bits should be consulted using WEP_GETPOS()
- `width`: Width of the match in lexemes minus 1, used for multi-lexeme matches

## Dependencies
- Used by callback functions:
  - [checkcondition_HL](../c/checkcondition_HL.md)
  - [hlCover](../h/hlCover.md)
  - [checkcondition_gin](../c/checkcondition_gin.md)
  - [checkcondition_arr](../c/checkcondition_arr.md)
  - [checkcondition_bit](../c/checkcondition_bit.md)
  - [checkcondition_QueryOperand](../c/checkcondition_QueryOperand.md)
  - [checkclass_str](../c/checkclass_str.md)
  - [checkcondition_str](../c/checkcondition_str.md)
- Used by phrase processing functions:
  - TS_phrase_output
  - [TS_phrase_execute](../T/TS_phrase_execute.md)
  - [TS_execute_locations_recurse](../T/TS_execute_locations_recurse.md)
- Related types:
  - WordEntryPos (lexeme position data type)
  - TSExecuteCallback (callback function type that uses this structure)

## Notes and Other Information
- Essential component of PostgreSQL's phrase-match operator implementation
- Position data must be sorted and contain unique values
- Callers should only access position bits via WEP_GETPOS() macro for proper bit extraction
- The pos array may point directly to WordEntryPos data from tsvector values for efficiency
- Used for both highlight generation and ranking calculations in full-text search
- Supports complex phrase matching scenarios including negation and multi-lexeme spans
- Memory management is indicated by the allocated flag - caller can free pos array when allocated is true
- All fields are zeroed initially, callback functions populate as needed