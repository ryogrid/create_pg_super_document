# WordEntryPosVector

## Location
[src/include/tsearch/ts_type.h:69-75](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/tsearch/ts_type.h#L69-L75)

## Overview
WordEntryPosVector is a variable-length structure that stores position and weight information for a word in PostgreSQL's text search vectors, containing an array of positional data entries.

## Definition
```c
typedef struct
{
    uint16      npos;
    WordEntryPos pos[FLEXIBLE_ARRAY_MEMBER];
} WordEntryPosVector;
```

## Detailed Description
WordEntryPosVector represents the positional information associated with a word in a tsvector. When a WordEntry has the haspos flag set to 1, it is followed by a WordEntryPosVector structure in the tsvector's data area. This structure contains:

- A count of position entries (npos)
- A flexible array of WordEntryPos values, each encoding both the position within the document and the weight (importance level) of the word at that position

The structure uses a flexible array member, allowing it to accommodate any number of position entries for a single word. Each position entry is a 16-bit value where the upper 2 bits store the weight (A, B, C, or D) and the lower 14 bits store the actual position within the document.

## Parameters / Member Variables
- `npos`: Number of position entries in the pos array (up to 65535 positions per word)
- `pos`: Flexible array of WordEntryPos values, each containing encoded position and weight information

## Dependencies
- Functions called/Symbols referenced:
  - WordEntryPos (position/weight data type)
- Used by (representative examples):
  - calc_rank_and (AND ranking calculations)
  - tsvector_unnest (extracting individual elements)
  - tsvector_filter (filtering by position/weight)
  - checkclass_str (classification checking)

## Notes and Other Information
- The flexible array member allows for variable-length position vectors without wasting space
- Position values are 1-based (position 0 is invalid)
- Positions are typically stored in ascending order within the array
- Weight values range from 0-3, corresponding to weights D, C, B, A respectively
- Maximum position value is 16383 (14-bit field), suitable for most document sizes
- This structure is only present when the corresponding WordEntry has haspos=1