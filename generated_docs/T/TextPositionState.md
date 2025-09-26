# TextPositionState

## Location
[src/backend/utils/adt/varlena.c:78-96](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L78-L96)

## Overview
TextPositionState is a structure that maintains state information for efficient text pattern searching operations using the Boyer-Moore-Horspool algorithm, with support for multibyte character handling and position tracking.

## Definition
```c
typedef struct
{
    bool        is_multibyte_char_in_char;  /* need to check char boundaries? */
    
    char       *str1;           /* haystack string */
    char       *str2;           /* needle string */
    int         len1;           /* string lengths in bytes */
    int         len2;
    
    /* Skip table for Boyer-Moore-Horspool search algorithm: */
    int         skiptablemask;  /* mask for ANDing with skiptable subscripts */
    int         skiptable[256]; /* skip distance for given mismatched char */
    
    char       *last_match;     /* pointer to last match in str1 */
    
    /*
     * Sometimes we need to convert the byte position of a match to a
     * character position.  These store the last position that was converted,
     * so that on the next call, we can continue from that point, rather than
     * count characters from the very beginning.
     */
    char       *refpoint;       /* pointer within original haystack string */
    int         refpos;         /* 0-based character offset of the same point */
} TextPositionState;
```

## Detailed Description
TextPositionState is a comprehensive state structure used for implementing efficient text searching functionality in PostgreSQL. It encapsulates all the necessary information for performing substring searches using the Boyer-Moore-Horspool algorithm, which provides better than linear time performance for most search patterns.

The structure handles both single-byte and multibyte character encodings, making it suitable for international text processing. It maintains skip tables for the Boyer-Moore-Horspool algorithm and tracks position information to optimize repeated searches and position calculations.

## Parameters / Member Variables
- `is_multibyte_char_in_char`: Boolean flag indicating whether multibyte character boundary checking is needed
- `str1`: Pointer to the haystack string (the text being searched in)
- `str2`: Pointer to the needle string (the pattern being searched for)
- `len1`: Length of the haystack string in bytes
- `len2`: Length of the needle string in bytes
- `skiptablemask`: Bit mask used for ANDing with skip table subscripts to handle table indexing
- `skiptable[256]`: Skip distance table for the Boyer-Moore-Horspool algorithm, indexed by character values
- `last_match`: Pointer to the location of the last successful match found in str1
- `refpoint`: Reference pointer within the original haystack string for position conversion optimization
- `refpos`: 0-based character offset corresponding to the refpoint location

## Dependencies
- Functions called/Symbols referenced:
  - (No direct function calls - this is a data structure)
- Called from (representative examples):
  - text_position
  - text_position_setup
  - text_position_next
  - text_position_next_internal
  - text_position_get_match_ptr
  - text_position_get_match_pos
  - text_position_reset
  - text_position_cleanup
  - replace_text
  - split_part
  - split_text

## Notes and Other Information
- The structure is optimized for repeated searches by caching position information and maintaining Boyer-Moore-Horspool skip tables
- The refpoint/refpos mechanism allows efficient conversion between byte positions and character positions without rescanning from the beginning
- The skip table uses a mask to handle character values outside the 0-255 range safely
- This structure is central to PostgreSQL's text processing functions like POSITION(), REPLACE(), and SPLIT_PART()
- The Boyer-Moore-Horspool algorithm provides O(n+m) average case performance for pattern searching