# compareWORD

## Location
src/backend/tsearch/to_tsany.c: 57 - 76

## Overview
A static comparison function used for sorting ParsedWord structures, comparing both the lexeme (word) and positional information to establish a total ordering.

## Definition


## Detailed Description
 is a comparison function designed for use with sorting algorithms (like ) to order  structures. It implements a two-level comparison strategy:

1. **Primary comparison**: Uses  to lexicographically compare the actual word content between two ParsedWord structures, considering text search normalization rules.

2. **Secondary comparison**: If the words are identical (primary comparison returns 0), it compares the positional information ( field) to ensure a stable, deterministic ordering even for duplicate words.

The function ensures that ParsedWord arrays can be sorted consistently, which is essential for text search vector creation where word order and position information must be preserved correctly.

## Parameters / Member Variables
- : Pointer to the first ParsedWord structure to compare (cast from void*)
- : Pointer to the second ParsedWord structure to compare (cast from void*)

Returns:
- Negative value if  should come before 
- Zero if  and  are equivalent
- Positive value if  should come after 

## Dependencies
- Functions called/Symbols referenced:
  - : Text search string comparison function that handles normalization
  - : Structure containing word text, length, and positional information
- Called from (representative examples):
  - : Uses this function for sorting before deduplication

## Notes and Other Information
- This is a static function internal to 
- The comparison strategy ensures that identical words are ordered by position, which is crucial for maintaining consistent text search vector representations
- Part of PostgreSQL's text search functionality, specifically used in tsvector creation
- The function signature follows the standard C comparison function convention used by 
- Located at lines 57-76 in 