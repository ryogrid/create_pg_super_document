# eq_v

## Location
src/backend/snowball/libstemmer/utilities.c: 225 - 228

## Overview
A convenience wrapper function that performs forward string matching for variable-length symbol arrays in Snowball stemming operations.

## Definition


## Detailed Description
The  function is a utility wrapper in the Snowball stemming framework that simplifies forward string matching for variable-length symbol arrays. It internally calls the  function, automatically extracting the size of the symbol array using the  macro.

The function works with Snowball's variable-length string representation where the length is stored immediately before the string data in memory. The  macro retrieves this length by accessing the integer value at . This allows  to match strings without requiring an explicit length parameter.

Like , this function matches text at the current cursor position moving forward, and advances the cursor on successful matches. It returns 1 for successful matches and 0 for failures.

## Parameters / Member Variables
- : Pointer to the Snowball environment structure containing the text buffer and cursor positions
- : Pointer to a variable-length symbol array (with size stored at )

## Dependencies
- Functions called/Symbols referenced:
  -  (forward string matching function)  
  -  (macro to extract length from variable-length arrays)
  -  (Snowball type definition)
- Called from (representative examples):
  -  function in header.h (used in multi-string matching operations)

## Notes and Other Information
- This is a convenience function that eliminates the need to manually specify string lengths
- Works specifically with Snowball's variable-length string format where size is stored at offset -1
- Part of the forward-matching family of functions in Snowball utilities
- Less commonly used compared to  as most stemming operations work with fixed-size patterns
- The underlying  function performs the actual matching and cursor advancement