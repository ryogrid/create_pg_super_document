# QueryRepresentationOperand

## Location
[src/backend/utils/adt/tsrank.c:548-553](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsrank.c#L548-L553)

## Overview
Represents the positional and existence data for a single operand within a text search query, used in PostgreSQL full-text search ranking calculations.

## Definition
```c
typedef struct
{
    bool        operandexists;
    bool        reverseinsert;    /* indicates insert order, true means
                                  * descending order */
    uint32      npos;
    WordEntryPos pos[MAXQROPOS];
} QueryRepresentationOperand;
```

## Detailed Description
QueryRepresentationOperand is a structure that stores metadata about individual operands (search terms) within a text search query for ranking purposes. It tracks whether an operand exists in a document, the positions where it occurs, and the order in which positions were inserted. This information is crucial for calculating text search relevance scores, particularly for positional ranking algorithms that consider word proximity and frequency.

The structure supports up to MAXQROPOS (defined as MAXENTRYPOS) positions per operand, allowing for efficient storage of word occurrence data while maintaining reasonable memory usage.

## Parameters / Member Variables
- `operandexists`: Boolean flag indicating whether this operand exists in the document being ranked
- `reverseinsert`: Boolean flag indicating the insertion order of positions (true means positions were inserted in descending order)
- `npos`: Number of positions stored in the pos array (must be ≤ MAXQROPOS)
- `pos[MAXQROPOS]`: Array storing the actual word positions where this operand occurs in the document

## Dependencies
- Functions called/Symbols referenced:
  - MAXQROPOS (macro definition)
  - WordEntryPos (type for position data)
- Called from (representative examples):
  - checkcondition_QueryOperand
  - fillQueryRepresentationData
  - calc_rank_cd

## Notes and Other Information
- Used primarily in text search ranking calculations within tsrank.c
- The reverseinsert flag is used to optimize position data access patterns during ranking
- Position data is stored in WordEntryPos format, which encodes both position and weight information
- Memory layout is designed for efficient access during ranking algorithm execution