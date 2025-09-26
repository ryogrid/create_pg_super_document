# text_position_setup

## Location
[src/backend/utils/adt/varlena.c:1216-1335](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L1216-L1335)

## Overview
The  function initializes the state for efficient substring searching, including Boyer-Moore-Horspool skip table preparation and collation handling.

## Definition

```c
static void
text_position_setup(text *t1, text *t2, Oid collid, TextPositionState *state)
```
## Detailed Description
The  function prepares the TextPositionState structure for efficient substring searching operations. It handles collation validation, multibyte encoding considerations, and most importantly, implements the Boyer-Moore-Horspool algorithm by building a skip table for fast pattern matching. The function determines the appropriate skip table size based on the search length and initializes it with optimal skip distances for each character in the pattern. It also handles special encoding cases, particularly UTF-8 and other multibyte encodings, to ensure correct character boundary detection during searches.

## Parameters / Member Variables
- : The text string to be searched (haystack)
- : The pattern to search for (needle)  
- : The collation ID for text comparison operations
- : Pointer to TextPositionState structure to initialize with search parameters

## Dependencies
- Functions called/Symbols referenced:
  -  - Validates the collation ID
  -  - Checks if collation is C locale
  -  - Creates locale from collation
  -  - Checks if locale is deterministic
  -  - Gets max character length for encoding
  -  - Retrieves current database encoding
  -  - Gets variable data size excluding header
  -  - Gets variable data pointer
- Called from (representative examples):
  -  - Core position search function
  -  - Text replacement operations
  -  - String splitting functions
  -  - Text splitting operations

## Notes and Other Information
- Implements Boyer-Moore-Horspool algorithm for efficient string searching
- Skip table size is dynamically determined based on search length (power of 2, max 256)
- Handles multibyte encodings with special consideration for UTF-8 optimization
- Validates that collation is deterministic (nondeterministic collations not supported)
- Performs raw byte sequence searching for most encodings, with character boundary verification for complex multibyte encodings
- Skip table uses bit-masking for fast element selection
- Designed for reuse across multiple searches with the same pattern