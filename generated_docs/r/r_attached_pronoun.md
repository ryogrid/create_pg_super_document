# r_attached_pronoun

## Location
src/backend/snowball/libstemmer/stem_UTF_8_spanish.c: 653 - 711

## Overview
This function removes attached pronouns from the end of words in the Snowball stemming algorithm for Romance languages (Catalan, Italian, Spanish).

## Definition
```c
static int r_attached_pronoun(struct SN_env * z)
```

## Detailed Description
The `r_attached_pronoun` function is part of the Snowball stemming algorithm implementation for Romance languages. It identifies and removes attached pronouns that appear as suffixes at the end of words. The function operates by:

1. Setting the ket (end) position to the current cursor position
2. Performing a character class check to ensure the character before the cursor is a valid letter
3. Using backward matching (find_among_b) against a predefined array of 39 attached pronoun patterns (a_1)
4. Setting the bra (start) position after a successful match
5. Verifying the match occurs within the R1 region (using r_R1 predicate)
6. Deleting the matched suffix if all conditions are met

This function is crucial for proper stemming of verbs with attached pronouns in Romance languages, where pronouns can be suffixed to verb forms (e.g., Spanish "dámelo" → "da").

## Parameters / Member Variables
- `z`: Pointer to the SN_env structure containing the stemming environment, including:
  - `c`: Current cursor position
  - `ket`: End position marker
  - `bra`: Start position marker  
  - `lb`: Left boundary
  - `p`: Pointer to the string being processed

## Dependencies
- Functions called/Symbols referenced:
  - r_R1 (tests if position is within R1 region)
  - find_among_b (backward pattern matching function)
  - slice_del (deletes text between bra and ket positions)
- Called from (representative examples):
  - catalan_ISO_8859_1_stem (src/backend/snowball/libstemmer/stem_ISO_8859_1_catalan.c:1404)
  - italian_ISO_8859_1_stem (src/backend/snowball/libstemmer/stem_ISO_8859_1_italian.c:980)
  - spanish_ISO_8859_1_stem (src/backend/snowball/libstemmer/stem_ISO_8859_1_spanish.c:992)

## Notes and Other Information
- The function uses bit manipulation for character class checking (checking if character is a letter)
- Returns 1 on successful removal, 0 if no match found, and negative values on error
- The a_1 array contains 39 different attached pronoun patterns specific to Romance languages
- This is a static function, indicating it's only used within the specific stemmer implementation file
- The function is replicated across multiple Romance language stemmer files with identical logic