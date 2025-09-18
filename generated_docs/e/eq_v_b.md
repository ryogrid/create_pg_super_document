# eq_v_b

## Location
[src/backend/snowball/libstemmer/utilities.c:229-232](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/utilities.c#L229-L232)

## Overview
A convenience wrapper function that performs backward string matching for variable-length symbol arrays in Snowball stemming operations.

## Definition


## Detailed Description
The  function is a utility wrapper in the Snowball stemming framework that simplifies backward string matching for variable-length symbol arrays. It internally calls the  function, automatically extracting the size of the symbol array using the  macro.

Similar to , this function works with Snowball's variable-length string representation where the length is stored immediately before the string data in memory. The  macro retrieves this length by accessing the integer value at . However, unlike , this function performs backward matching from the current cursor position.

The function checks if the pattern matches the text immediately before the current cursor position, and if successful, moves the cursor backward by the length of the matched pattern. It returns 1 for successful matches and 0 for failures.

## Parameters / Member Variables
- : Pointer to the Snowball environment structure containing the text buffer and cursor positions
- : Pointer to a variable-length symbol array (with size stored at )

## Dependencies
- Functions called/Symbols referenced:
  -  (backward string matching function)
  -  (macro to extract length from variable-length arrays)  
  -  (Snowball type definition)
- Called from (representative examples):
  -  in Danish stemmer (ISO_8859_1 and UTF_8 variants)
  -  in Finnish stemmer (ISO_8859_1 and UTF_8 variants)
  -  function in header.h (used in multi-string matching operations)

## Notes and Other Information
- This is the backward-matching counterpart to 
- Eliminates the need to manually specify string lengths for backward matching operations
- Works specifically with Snowball's variable-length string format where size is stored at offset -1
- Used in suffix removal and cleanup operations in various language stemmers
- The underlying  function performs the actual backward matching and cursor movement
- Part of the backward-matching family of functions in Snowball utilities