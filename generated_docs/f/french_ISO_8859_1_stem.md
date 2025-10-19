# french_ISO_8859_1_stem

## Location
[src/backend/snowball/libstemmer/stem_ISO_8859_1_french.c:1153-1248](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_ISO_8859_1_french.c#L1153-L1248)

## Overview  
The french_ISO_8859_1_stem function is the main entry point for French word stemming using the Snowball algorithm for ISO-8859-1 encoded text.

## Definition

```c
}

extern int french_ISO_8859_1_stem(struct SN_env * z)
```
## Detailed Description
This function implements the complete French stemming algorithm by orchestrating the following phases:
1. **Prelude**: Character preprocessing and normalization (r_prelude)  
2. **Region marking**: Identifies morphological boundaries R1, R2, RV (r_mark_regions)
3. **Suffix removal**: Attempts suffix removal in priority order:
   - Standard suffixes (r_standard_suffix)
   - Verb suffixes starting with 'i' (r_i_verb_suffix) 
   - General verb suffixes (r_verb_suffix)
   - Residual suffixes (r_residual_suffix)
4. **Character corrections**: Handles special case replacements:
   - 'Y' → 'i' (s_33)
   - 'ç' (0xE7) → 'c' (s_34)
5. **Cleanup operations**:
   - Undouble consonants (r_un_double)
   - Remove accents (r_un_accent)  
6. **Postlude**: Final character case processing (r_postlude)

The function uses a sophisticated backtracking mechanism to try different suffix removal strategies in priority order.

## Parameters / Member Variables
- `*z`: Pointer to the Snowball environment structure containing:
## Dependencies
- Functions called/Symbols referenced:
  - [r_prelude](../r/r_prelude.md) (text preprocessing)
  - [r_mark_regions](../r/r_mark_regions.md) (morphological boundary identification)
  - [r_standard_suffix](../r/r_standard_suffix.md) (standard suffix removal)
  - [r_i_verb_suffix](../r/r_i_verb_suffix.md) (verb suffixes starting with 'i')
  - [r_verb_suffix](../r/r_verb_suffix.md) (general verb suffix removal)
  - [r_residual_suffix](../r/r_residual_suffix.md) (residual suffix handling)
  - [r_un_double](../r/r_un_double.md) (consonant undoubling)
  - [r_un_accent](../r/r_un_accent.md) (accent removal)
  - [r_postlude](../r/r_postlude.md) (final processing)
  - [slice_from_s](../s/slice_from_s.md) (string replacement)
  - s_33 (replacement string 'i')
  - s_34 (replacement string 'c')

- Called from (representative examples):
  - External stemming interfaces
  - PostgreSQL full-text search dictionaries

## Notes and Other Information
- This is the main entry point for French stemming in PostgreSQL's full-text search
- Designed specifically for ISO-8859-1 character encoding (Western European)  
- Uses multiple cursor position save/restore operations to handle backtracking
- The algorithm follows the official Snowball French stemmer specification
- Returns 1 on successful completion, negative values indicate errors
- The function modifies the input text in-place within the SN_env structure
- Character constants like 0xE7 (ç) are specific to ISO-8859-1 encoding
- Processing order is critical: suffix removal before cleanup operations

## Simplified Source

```c
extern int french_ISO_8859_1_stem(struct SN_env * z) {
    // Phase 1: Character preprocessing
    int start_pos = z->c;
    r_prelude(z);
    z->c = start_pos;

    // Phase 2: Mark morphological regions (R1, R2, RV)
    r_mark_regions(z);

    // Phase 3: Process from end of word
    z->lb = z->c;
    z->c = z->l;

    // Phase 4: Try suffix removal in priority order
    int suffix_pos = z->l - z->c;

    // Try standard suffixes first
    if (r_standard_suffix(z) == 0) {
        // If no standard suffix, try verb suffixes
        z->c = z->l - suffix_pos;
        if (r_i_verb_suffix(z) == 0) {
            z->c = z->l - suffix_pos;
            if (r_verb_suffix(z) == 0) {
                // Try residual suffixes as last resort
                z->c = z->l - suffix_pos;
                r_residual_suffix(z);
            }
        }
    }

    // Phase 5: Character corrections (Y→i, ç→c)
    z->c = z->l - suffix_pos;
    z->ket = z->c;
    if (z->c > z->lb && z->p[z->c - 1] == 'Y') {
        z->c--;
        z->bra = z->c;
        slice_from_s(z, 1, s_33);  // Replace with 'i'
    } else if (z->c > z->lb && z->p[z->c - 1] == 0xE7) {
        z->c--;
        z->bra = z->c;
        slice_from_s(z, 1, s_34);  // Replace with 'c'
    }

    // Phase 6: Cleanup operations
    int cleanup_pos = z->l - z->c;
    r_un_double(z);  // Remove doubled consonants
    z->c = z->l - cleanup_pos;
    r_un_accent(z);  // Remove accents

    // Phase 7: Final processing
    z->c = z->lb;
    int final_pos = z->c;
    r_postlude(z);
    z->c = final_pos;

    return 1;
}
```