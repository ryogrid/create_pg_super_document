# r_Checks1

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_arabic.c:1022-1045](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_arabic.c#L1022-L1045)

## Overview
This function performs initial validation checks for Arabic text in the PostgreSQL Snowball stemmer, determining if the word meets criteria for further stemming processing.

## Definition
static int r_Checks1(struct SN_env * z)

## Detailed Description
The r_Checks1 function is a validation routine in the Arabic UTF-8 stemming algorithm that performs preliminary checks on the input word to determine if it should undergo stemming. It uses lookup table a_3 (4 entries) to identify specific Arabic patterns at the current cursor position.

The function checks for two main pattern categories:
- **Case 1**: Requires the word to have more than 4 UTF-8 characters in length
- **Case 2**: Requires the word to have more than 3 UTF-8 characters in length

When a valid pattern is found and length requirements are met, the function sets internal state variables in the stemming environment:
- I[2] = 1: Indicates a valid pattern was found
- I[1] = 0: Resets intermediate state  
- I[0] = 1: Sets processing flag

The function also performs a preliminary character check, ensuring the character at position c+3 matches specific Arabic character codes (132 or 167).

## Parameters / Member Variables
- : Pointer to SN_env structure containing the stemming environment with the input string, cursor positions, integer state variables (I[]), and working buffers

## Dependencies
- Functions called/Symbols referenced:
  - [find_among](../f/find_among.md): Searches for patterns using lookup table a_3
  - [len_utf8](../l/len_utf8.md): Calculates UTF-8 character length of the string
- Called from (representative examples):
  - [arabic_UTF_8_stem](../a/arabic_UTF_8_stem.md): Main Arabic stemming function at src/backend/snowball/libstemmer/stem_UTF_8_arabic.c:1419

## Notes and Other Information
This function serves as a gatekeeper for the Arabic stemming process, ensuring only words that meet specific pattern and length criteria proceed to more complex stemming operations. The function returns 1 if checks pass and the word should be stemmed, or 0 if the word should be left unchanged. The state variables set by this function guide subsequent stemming steps in the algorithm.

## Simplified Source

```c
static int r_Checks1(struct SN_env * z) {
    // Set boundary markers
    z->bra = z->c;

    // Pre-check: verify character at position c+3 is valid Arabic
    if (z->c + 3 >= z->l ||
        (z->p[z->c + 3] != 132 && z->p[z->c + 3] != 167)) {
        return 0; // Invalid character or out of bounds
    }

    // Find Arabic pattern using lookup table
    int pattern = find_among(z, a_3, 4);
    if (!pattern) {
        return 0; // No valid pattern found
    }

    z->ket = z->c;

    // Check pattern and word length requirements
    switch (pattern) {
        case 1: // Pattern requires word length > 4 characters
            if (len_utf8(z->p) <= 4) return 0;
            break;

        case 2: // Pattern requires word length > 3 characters
            if (len_utf8(z->p) <= 3) return 0;
            break;

        default:
            return 0; // Unknown pattern
    }

    // Set stemming state flags
    z->I[2] = 1; // Valid pattern found
    z->I[1] = 0; // Reset intermediate state
    z->I[0] = 1; // Enable processing

    return 1; // Checks passed
}
```