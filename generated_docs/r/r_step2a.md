# r_step2a

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_greek.c:2880-2903](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_greek.c#L2880-L2903)

## Overview
A static function that implements step 2a of the Greek language stemming algorithm, performing conditional suffix removal with validation checks and string insertion operations.

## Definition
static int r_step2a(struct SN_env * z)

## Detailed Description
The r_step2a function performs morphological transformations for step 2a in Greek stemming with specific validation and conditional processing:

1. Sets the cursor position (ket) to current position
2. Performs bounds checking (minimum 7 characters from left boundary)
3. Validates that the character at position c-1 is either 131 or 189 (specific Greek UTF-8 character codes)
4. Uses find_among_b to search backward through predefined suffix patterns (a_24 array with 2 entries)
5. If a match is found, deletes the matched suffix using slice_del
6. Performs a negative validation check using a_25 array (10 entries) - returns 0 if patterns are found
7. If validation passes, inserts a 4-character string (s_65) at the current cursor position

The function uniquely combines suffix removal with string insertion, and includes negative pattern matching to prevent inappropriate transformations.

## Parameters / Member Variables
- `z`: Pointer to SN_env structure containing the stemming environment, including:

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md) (backward string pattern matching)
  - [slice_del](../s/slice_del.md) (suffix deletion)
  - [insert_s](../i/insert_s.md) (string insertion)
- Called from (representative examples):
  - [greek_UTF_8_stem](../g/greek_UTF_8_stem.md)

## Notes and Other Information
- Returns 1 on successful execution, 0 on validation failure or negative pattern match
- Part of automatically generated Snowball stemmer code for Greek language
- Uses character validation for specific Greek UTF-8 codes (131, 189)
- Implements negative validation using a_25 array to prevent incorrect transformations
- Combines suffix removal with string insertion, making it distinct from other step functions
- Uses predefined arrays (a_24, a_25) and string constant (s_65)

## Simplified Source

```c
static int r_step2a(struct SN_env * z) {
    // Initial validation and pattern matching
    z->ket = z->c;

    // Check bounds (need at least 7 characters) and specific characters
    if (z->c - 7 <= z->lb) return 0;
    char last_char = z->p[z->c - 1];
    if (last_char != 131 && last_char != 189) return 0;  // Check for ƒ or ½

    // Find and delete suffix patterns from array a_24
    if (!(find_among_b(z, a_24, 2))) return 0;
    z->bra = z->c;
    slice_del(z);  // Delete matched suffix

    // Negative validation: ensure no patterns from a_25 are present
    int saved_pos = z->l - z->c;
    if (find_among_b(z, a_25, 10)) {
        return 0;  // Abort if negative patterns found
    }
    z->c = z->l - saved_pos;  // Restore position

    // Insert 4-character string at current position
    int saved_cursor = z->c;
    insert_s(z, z->c, z->c, 4, s_65);
    z->c = saved_cursor;  // Restore cursor position

    return 1;  // Success
}
```