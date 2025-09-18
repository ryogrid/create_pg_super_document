# among

## Location
src/include/snowball/libstemmer/header.h: 15 - 61

## Overview
The  struct is a data structure used in the Snowball stemming library to represent string lookup tables for pattern matching and text transformation operations.

## Definition


## Detailed Description
The  struct serves as an entry in a lookup table used by the Snowball stemming algorithm. It represents a search pattern with associated metadata that enables efficient string matching and transformation. Each  entry contains a search string, its length, substring information for optimization, a result value, and an optional function pointer for custom processing. This structure is primarily used by the  and  functions to perform binary search operations on sorted arrays of these structures.

## Parameters / Member Variables
- : The number of characters in the search string, used for length comparison and bounds checking
- : Pointer to the search string (symbol array) that this entry represents
- : Index to the longest matching substring entry, used for optimization in the search algorithm (-1 if no substring)
- : The result value to return when this pattern matches successfully
- : Optional function pointer that gets called when this pattern matches; if null, the result is returned directly

## Dependencies
- Functions called/Symbols referenced:
  - symbol (type used for string representation)
  - SN_env (struct passed to function pointer)
- Called from (representative examples):
  - find_among (src/backend/snowball/libstemmer/utilities.c:233)
  - find_among_b (src/backend/snowball/libstemmer/utilities.c:298)

## Notes and Other Information
This structure is part of the Snowball stemming library integrated into PostgreSQL for text search functionality. The  arrays are typically sorted by their search strings to enable efficient binary search operations. The substring_i field creates a chain of fallback patterns, allowing the algorithm to find the longest possible match when exact matches fail. The function pointer mechanism allows for dynamic processing of matches, enabling complex stemming rules that go beyond simple string replacement.