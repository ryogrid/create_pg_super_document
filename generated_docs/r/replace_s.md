# replace_s

## Location
src/backend/snowball/libstemmer/utilities.c: 374 - 404

## Overview
Replaces a segment of symbols in a Snowball environment's string buffer with a new sequence of symbols, handling memory reallocation and cursor position adjustments as needed.

## Definition


## Detailed Description
The `replace_s` function performs string replacement operations in PostgreSQL's Snowball stemming environment. It replaces symbols between positions `c_bra` (bracket start) and `c_ket` (bracket end) in the environment's string buffer (`z->p`) with `s_size` symbols from the source array `s`. The function handles dynamic memory management by calling `increase_size` when the replacement requires more space than currently available. It also manages cursor position adjustments and calculates the size difference between the original and replacement text. On error, the function frees the buffer and sets it to NULL.

## Parameters / Member Variables
- `z`: Pointer to the Snowball environment structure containing the string buffer and state
- `c_bra`: Start position (bracket start) of the segment to be replaced
- `c_ket`: End position (bracket end) of the segment to be replaced
- `s_size`: Number of symbols in the replacement sequence
- `s`: Pointer to the array of replacement symbols
- `adjptr`: Optional pointer to store the size adjustment value (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - create_s
  - increase_size
  - SIZE (macro for getting buffer size)
  - CAPACITY (macro for getting buffer capacity)
  - SET_SIZE (macro for setting buffer size)
  - memmove
  - symbol (type definition)
- Called from (representative examples):
  - SN_set_current
  - slice_from_s
  - insert_s
  - among

## Notes and Other Information
- This is an external function accessible from other modules in the Snowball stemmer
- Returns 0 on success, -1 on error
- Automatically creates a new string buffer if `z->p` is NULL
- Handles cursor position (`z->c`) and string length (`z->l`) adjustments automatically
- Uses efficient `memmove` operations for shifting existing symbols
- Critical function for text manipulation in stemming algorithms