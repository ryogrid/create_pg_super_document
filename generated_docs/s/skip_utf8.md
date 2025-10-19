# skip_utf8

## Location
[src/backend/snowball/libstemmer/utilities.c:27-51](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/utilities.c#L27-L51)

## Overview
Advances a position pointer forward by n UTF-8 characters within a symbol buffer, properly handling multi-byte UTF-8 character sequences.

## Definition

```c
*/

extern int skip_utf8(const symbol * p, int c, int limit, int n)
```
## Detailed Description
The  function is a critical UTF-8 text processing utility in PostgreSQL's Snowball stemming library. It advances a character position forward by exactly n UTF-8 characters, correctly handling multi-byte character sequences. This function is essential for proper text navigation in languages that use non-ASCII characters, ensuring that character boundaries are respected rather than just advancing byte positions. It implements the logic for UTF-8 character skipping used by the 'hop' and 'next' operations in UTF-8 stemming algorithms.

## Parameters / Member Variables
- :  - Pointer to the symbol buffer containing UTF-8 encoded text
- :  - Current character position (byte offset) to start from
- :  - Maximum allowed position (boundary limit)  
- :  - Number of UTF-8 characters to skip forward
- Returns:  - New character position after skipping n characters, or -1 on failure

## Dependencies
- Functions called/Symbols referenced:
  -  - Symbol structure type for text storage

- Called from (representative examples):
  - Various UTF-8 stemming functions across multiple language modules:
  -  (Arabic stemmer)
  -  (Basque, Danish, French, German, etc. stemmers)
  -  (Dutch, English, French, German, Italian, etc. stemmers)
  -  (Dutch, English, French, German, Italian, etc. stemmers)
  -  (src/include/snowball/libstemmer/header.h:26)

## Notes and Other Information
- Returns -1 if n is negative or if position would exceed the limit
- Correctly handles UTF-8 multi-byte sequences by checking byte patterns (0xC0, 0x80 ranges)
- Essential for proper character navigation in international text processing
- Used extensively across all UTF-8 language stemming modules in PostgreSQL
- Ensures character-level rather than byte-level text traversal for proper linguistic processing

## Simplified Source

```c
extern int skip_utf8(const symbol * p, int c, int limit, int n) {
    // Input validation
    if (n < 0) return -1;

    // Skip n UTF-8 characters forward
    for (; n > 0; n--) {
        if (c >= limit) return -1;  // Boundary check

        int b = p[c++];  // Get next byte

        // If this is a multi-byte UTF-8 character (starts with 11...)
        if (b >= 0xC0) {
            // Skip continuation bytes (10......)
            while (c < limit) {
                b = p[c];
                if (b >= 0xC0 || b < 0x80) break;  // Not a continuation byte
                c++;
            }
        }
    }
    return c;
}
```