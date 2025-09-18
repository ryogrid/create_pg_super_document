# silly_cmp_tsvector

## Location
[src/backend/utils/adt/tsvector_op.c:86-144](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_op.c#L86-L144)

## Overview
Internal comparison function for TSVector values that implements a comprehensive ordering based on size, lexeme count, word entries, and positional information with weights.

## Definition
```c
static int silly_cmp_tsvector(const TSVector a, const TSVector b)
```

## Detailed Description
This static function provides a complete comparison mechanism for TSVector data structures, establishing a deterministic ordering for text search vectors. The comparison follows a hierarchical approach:

1. **Size comparison**: First compares the overall VARSIZE of the vectors
2. **Lexeme count**: Compares the number of distinct lexemes (a->size vs b->size) 
3. **Word-by-word comparison**: For each lexeme, compares:
   - Position information availability (haspos flag)
   - Lexeme string content using tsCompareString
   - Position data length and individual positions with weights

The function name "silly" likely refers to its comprehensive nature - it compares every aspect of the TSVector structure to establish a total ordering, which may be overkill for some use cases but ensures consistent results.

## Parameters / Member Variables
- `a`: First TSVector to compare (const pointer)
- `b`: Second TSVector to compare (const pointer)

## Dependencies
- Functions called/Symbols referenced:
  - VARSIZE (macro for getting variable-length data size)
  - ARRPTR (macro for getting WordEntry array pointer)
  - [tsCompareString](../t/tsCompareString.md) (string comparison function)
  - STRPTR (macro for getting string data pointer)
  - POSDATAPTR (macro for getting position data pointer)
  - POSDATALEN (macro for getting position data length)
  - WEP_GETPOS (macro for extracting position from WordEntryPos)
  - WEP_GETWEIGHT (macro for extracting weight from WordEntryPos)
- Called from (representative examples):
  - TSVECTORCMPFUNC (macro/function pointer assignment)

## Notes and Other Information
- Returns -1 if a < b, 1 if a > b, 0 if a == b
- The comparison establishes a total ordering suitable for sorting and indexing operations
- Handles both TSVectors with and without positional information
- The hierarchical comparison ensures consistent ordering even for complex TSVector structures
- Used internally by PostgreSQL for TSVector comparison operations