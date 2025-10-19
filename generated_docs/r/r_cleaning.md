# r_cleaning

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_catalan.c:1215-1269](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_catalan.c#L1215-L1269)

## Overview
r_cleaning is a static function that performs character normalization and cleaning operations on words as part of the Catalan language stemming process, replacing specific character combinations with their canonical forms.

## Definition

```c
}

static int r_cleaning(struct SN_env * z)
```
## Detailed Description
This function implements a character cleaning and normalization step for Catalan text processing within the Snowball stemming framework. It operates by continuously searching for specific patterns in the input word and replacing them with normalized equivalents. The function uses a pattern matching approach with the find_among function to identify character sequences that need cleaning.

The function processes the word from the current cursor position, setting boundaries (bra/ket) around matched patterns and performing replacements through a switch statement. It handles 7 different cases:
- Cases 1-6: Replace matched sequences with predefined strings (s_0 through s_5)
- Case 7: Skip one character forward

The cleaning loop continues until no more patterns are found, ensuring all applicable normalizations are performed in a single pass.

## Parameters / Member Variables
- : Pointer to the Snowball environment structure containing:
  - : Current cursor position in the word
  - : Length of the word being processed
  - : Start boundary marker for pattern matching
  - : End boundary marker for pattern matching

## Dependencies
- Functions called/Symbols referenced:
  - [find_among](../f/find_among.md) (for pattern matching against array a_0 with 13 elements)
  - [slice_from_s](../s/slice_from_s.md) (for string replacements, called 6 times)
- Called from (representative examples):
  - [catalan_ISO_8859_1_stem](../c/catalan_ISO_8859_1_stem.md) (src/backend/snowball/libstemmer/stem_ISO_8859_1_catalan.c:1435)
  - [catalan_UTF_8_stem](../c/catalan_UTF_8_stem.md) (src/backend/snowball/libstemmer/stem_UTF_8_catalan.c:1438)

## Notes and Other Information
- This function is specific to Catalan language processing and implements language-specific character normalization rules
- The cleaning process is essential for proper stemming as it standardizes character representations before morphological analysis
- The function uses a continuous loop structure to ensure all applicable cleanings are performed in sequence
- Returns 1 on successful completion, following the Snowball framework convention
- Part of the preprocessing pipeline that runs before the main stemming algorithms
- The specific character mappings (s_0 through s_5) contain the actual replacement strings defined elsewhere in the Catalan stemmer

## Simplified Source

```c
static int r_cleaning(struct SN_env * z) {
    // Continuous loop to normalize characters
    while(1) {
        int saved_cursor = z->c;

        // Find character pattern needing normalization
        z->bra = z->c;
        int pattern = find_among(z, a_0, 13);
        if (!pattern) {
            z->c = saved_cursor;
            break;  // No more patterns found
        }

        z->ket = z->c;

        // Apply appropriate character replacement
        switch (pattern) {
            case 1: case 2: case 3: case 4: case 5: case 6:
                // Replace with normalized character (s_0 through s_5)
                slice_from_s(z, 1, normalized_chars[pattern-1]);
                break;
            case 7:
                // Skip this character
                z->c++;
                break;
        }
    }
    return 1;
}
```