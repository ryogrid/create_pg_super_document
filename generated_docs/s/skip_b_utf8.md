# skip_b_utf8

## Location
src/backend/snowball/libstemmer/utilities.c: 52 - 70

## Overview
Advances a position pointer backward by n UTF-8 characters within a symbol buffer, properly handling multi-byte UTF-8 character sequences in reverse direction.

## Definition


## Detailed Description
The  function is the backward counterpart to , providing UTF-8-aware backward character navigation in PostgreSQL's Snowball stemming library. It moves a character position backward by exactly n UTF-8 characters, correctly handling multi-byte character sequences when traversing in reverse. This function is essential for stemming algorithms that need to examine text patterns from right to left, ensuring that UTF-8 character boundaries are properly respected during backward traversal. It implements the backward navigation logic for 'hop' and 'next' operations in UTF-8 stemming algorithms.

## Parameters / Member Variables
- :  - Pointer to the symbol buffer containing UTF-8 encoded text
- :  - Current character position (byte offset) to start from
- :  - Minimum allowed position (lower boundary limit)
- :  - Number of UTF-8 characters to skip backward
- Returns:  - New character position after skipping n characters backward, or -1 on failure

## Dependencies
- Functions called/Symbols referenced:
  -  - Symbol structure type for text storage

- Called from (representative examples):
  - Various UTF-8 stemming functions across multiple language modules:
  -  (Arabic stemmer)
  -  (Danish, Norwegian, Swedish stemmers)
  -  (Dutch, Hungarian stemmers)
  - ,  (English, Porter stemmers)
  -  (German, Yiddish stemmers)
  -  (Greek stemmer)
  -  (src/include/snowball/libstemmer/header.h:28)

## Notes and Other Information
- Returns -1 if n is negative or if position would go below the limit
- Correctly handles UTF-8 multi-byte sequences by checking byte patterns (0x80, 0xC0 ranges) in reverse
- Essential for backward pattern matching and suffix analysis in stemming algorithms
- Used extensively across UTF-8 language stemming modules for suffix removal and pattern detection
- Ensures character-level backward traversal rather than byte-level for proper linguistic processing
- Particularly important for languages with complex morphological endings