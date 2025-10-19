# r_stem_nominal_verb_suffixes

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_turkish.c:901-1157](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_turkish.c#L901-L1157)

## Overview
Main function for identifying and removing nominal verb suffixes in Turkish text as part of the Snowball stemming algorithm, handling complex suffix chains and morphological patterns.

## Definition

```c
}

static int r_stem_nominal_verb_suffixes(struct SN_env * z)
```
## Detailed Description
This function is the core component for processing nominal verb suffixes in Turkish morphological analysis. It implements a sophisticated algorithm that handles multiple layers of suffix combinations, including past participles (-ymUs̈), present tense markers (-yDU), conditional forms (-ysA), temporal forms (-yken), and various person/number agreement markers.

The function uses a complex branching structure with multiple fallback paths to identify and process different suffix patterns. It maintains cursor positions and uses backtracking to handle the agglutinative nature of Turkish morphology, where multiple suffixes can be stacked on a single word stem. The algorithm sets boundary markers (ket/bra) and removes identified suffixes while maintaining grammatical coherence.

Key processing phases include:
1. Initial suffix identification (participles, tense markers)
2. Person/number agreement processing (1st/2nd/3rd person, singular/plural)
3. Suffix chain validation and removal
4. Secondary suffix processing after initial removals

## Parameters / Member Variables  
- : Pointer to the Snowball environment structure containing:
  - : Current cursor position
  - : Length/end position of string
  - /: Boundary markers for suffix identification
  - : Integer flag for tracking processing state
  - : Character array being processed

## Dependencies
- Functions called/Symbols referenced:
  - : Identifies past participle suffixes (-miş, -muş, -mış, -müş)
  - : Identifies past tense markers
  - : Identifies conditional suffixes  
  - : Identifies temporal 'while/when' suffixes
  - : Identifies 'as if' conditional markers
  - : Identifies 2nd person plural markers
  - : Identifies plural/3rd person markers
  - : Identifies 1st person singular markers
  - : Identifies 2nd person singular markers  
  - : Identifies 2nd person plural markers
  - : Identifies 1st person plural markers
  - : Identifies present tense 3rd person markers
  - : Removes identified suffix segments

- Called from:
  - : Main Turkish stemming function

## Notes and Other Information
- Returns 1 on successful processing, negative values on errors
- Uses extensive branching logic with labeled gotos for complex suffix pattern matching
- Handles Turkish vowel harmony and consonant insertion rules through subsidiary functions
- Manages cursor position restoration using saved positions (m1-m10 variables)
- Sets I[0] flag to track whether certain suffix types were processed
- Critical component of Turkish text normalization and search functionality
- Processes suffixes in reverse order (right-to-left) due to agglutinative word structure
- Implements morphological decomposition essential for Turkish language processing

## Simplified Source

```c
static int r_stem_nominal_verb_suffixes(struct SN_env * z) {
    z->ket = z->c;
    z->I[0] = 1;

    // Phase 1: Try to match primary nominal verb suffixes
    // Try past participle (-ymUs̈), past tense (-yDU), conditional (-ysA), or temporal (-yken)
    if (r_mark_ymUs_(z) || r_mark_yDU(z) || r_mark_ysA(z) || r_mark_yken(z)) {
        // Found primary suffix, process it
        goto remove_suffix;
    }

    // Phase 2: Try conditional suffix with person markers
    if (r_mark_cAsInA(z)) {
        // Try person/number markers: sUnUz, lAr, yUm, sUn, yUz
        r_mark_sUnUz(z) || r_mark_lAr(z) || r_mark_yUm(z) ||
        r_mark_sUn(z) || r_mark_yUz(z);

        // Must be followed by past participle
        if (r_mark_ymUs_(z)) {
            goto remove_suffix;
        }
    }

    // Phase 3: Try plural marker with tense suffixes
    if (r_mark_lAr(z)) {
        slice_del(z);  // Remove the lAr suffix

        z->ket = z->c;
        // Try tense markers after removing lAr
        r_mark_DUr(z) || r_mark_yDU(z) || r_mark_ysA(z) || r_mark_ymUs_(z);
        z->I[0] = 0;
        goto remove_suffix;
    }

    // Phase 4: Try 1st person plural with tense markers
    if (r_mark_nUz(z)) {
        r_mark_yDU(z) || r_mark_ysA(z);
        goto remove_suffix;
    }

    // Phase 5: Try other person markers
    if (r_mark_sUnUz(z) || r_mark_yUz(z) || r_mark_sUn(z) || r_mark_yUm(z)) {
        slice_del(z);  // Remove person marker

        z->ket = z->c;
        r_mark_ymUs_(z);  // Try past participle
        goto remove_suffix;
    }

    // Phase 6: Try present tense marker
    if (r_mark_DUr(z)) {
        slice_del(z);

        z->ket = z->c;
        // Try person markers after present tense
        r_mark_sUnUz(z) || r_mark_lAr(z) || r_mark_yUm(z) ||
        r_mark_sUn(z) || r_mark_yUz(z);

        r_mark_ymUs_(z);  // Try past participle
    }

remove_suffix:
    z->bra = z->c;
    slice_del(z);  // Remove the identified suffix
    return 1;
}
```