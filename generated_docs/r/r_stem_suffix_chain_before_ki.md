# r_stem_suffix_chain_before_ki

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_turkish.c:1158-1353](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_turkish.c#L1158-L1353)

## Overview
Processes complex suffix chains that appear before the Turkish relativizer suffix "-ki" by recursively identifying and removing layered morphological patterns.

## Definition

```c
}

static int r_stem_suffix_chain_before_ki(struct SN_env * z)
```
## Detailed Description
This function handles the intricate morphological patterns that precede the Turkish suffix "-ki" (meaning 'that which' or used as a relativizer). Turkish grammar allows multiple suffixes to be stacked before "-ki", creating complex chains that need to be systematically identified and processed.

The function implements a recursive algorithm with three main processing branches:
1. **Locative branch (-DA)**: Handles locative case suffixes followed by optional plural markers and possessives
2. **Genitive branch (-nUn)**: Processes genitive case markers with subsequent possessive and plural patterns  
3. **Locative variant branch (-ndA)**: Manages alternative locative forms with recursive processing

The algorithm uses backtracking and cursor position management to handle the recursive nature of Turkish suffix stacking. It identifies specific suffix patterns, removes them via slice_del operations, and recursively processes remaining suffix chains until no more patterns are found.

## Parameters / Member Variables
- : Pointer to the Snowball environment structure containing:
  - : Current cursor position in the string
  - : Length/end position of string  
  - /: Boundary markers for identifying suffix boundaries
  - : Character array being processed

## Dependencies
- Functions called/Symbols referenced:
  - : Identifies the "-ki" relativizer suffix
  - : Identifies locative case suffixes (-da/-de)
  - : Identifies genitive case suffix (-nun/-nün)
  - : Identifies alternative locative forms (-nda/-nde)
  - : Identifies plural markers (-lar/-ler)
  - : Identifies plural possessive combinations
  - : Identifies possessive suffixes (various forms)
  - : Identifies 3rd person possessive markers
  - : Removes identified suffix segments
  - **Self-recursive**: Calls itself to process nested suffix chains

- Called from:
  - **Self (recursive calls)**: For processing nested suffix patterns
  - : Main noun suffix processing function (multiple locations)

## Notes and Other Information
- Returns 1 on successful processing, 0 or negative values on failure/errors
- Implements recursive descent parsing for Turkish morphological analysis
- Uses extensive cursor position saving/restoration (m1-m11 variables) for backtracking
- Essential for handling Turkish's agglutinative morphology before relativizer constructions
- The recursive nature reflects the nested structure of Turkish suffix combinations
- Manages complex branching logic with labeled gotos for efficient pattern matching
- Critical for proper noun phrase analysis in Turkish text processing
- Handles vowel harmony through subsidiary marking functions

## Simplified Source

```c
static int r_stem_suffix_chain_before_ki(struct SN_env * z) {
    z->ket = z->c;

    // Must start with "-ki" suffix
    int ret = r_mark_ki(z);
    if (ret <= 0) return ret;

    // Branch 1: Handle locative case (-DA) patterns
    if (r_mark_DA(z)) {
        z->bra = z->c;
        slice_del(z);  // Remove DA suffix

        z->ket = z->c;

        // Try plural marker after locative
        if (r_mark_lAr(z)) {
            z->bra = z->c;
            slice_del(z);  // Remove lAr

            // Recursively process any remaining suffix chains
            r_stem_suffix_chain_before_ki(z);
        }
        // Try possessive markers after locative
        else if (r_mark_possessives(z)) {
            z->bra = z->c;
            slice_del(z);  // Remove possessive

            z->ket = z->c;
            // Optional plural after possessive
            if (r_mark_lAr(z)) {
                z->bra = z->c;
                slice_del(z);
                r_stem_suffix_chain_before_ki(z);
            }
        }
        return 1;
    }

    // Branch 2: Handle genitive case (-nUn) patterns
    if (r_mark_nUn(z)) {
        z->bra = z->c;
        slice_del(z);  // Remove nUn suffix

        z->ket = z->c;

        // Try possessive plural combination
        if (r_mark_lArI(z)) {
            z->bra = z->c;
            slice_del(z);
        }
        // Try possessives or 3rd person marker
        else if (r_mark_possessives(z) || r_mark_sU(z)) {
            z->bra = z->c;
            slice_del(z);

            z->ket = z->c;
            // Optional plural
            if (r_mark_lAr(z)) {
                z->bra = z->c;
                slice_del(z);
                r_stem_suffix_chain_before_ki(z);
            }
        }
        else {
            // Continue processing remaining chains
            r_stem_suffix_chain_before_ki(z);
        }
        return 1;
    }

    // Branch 3: Handle alternative locative (-ndA) patterns
    if (r_mark_ndA(z)) {
        // Try possessive plural or 3rd person marker
        if (r_mark_lArI(z)) {
            z->bra = z->c;
            slice_del(z);
        }
        else if (r_mark_sU(z)) {
            z->bra = z->c;
            slice_del(z);

            z->ket = z->c;
            if (r_mark_lAr(z)) {
                z->bra = z->c;
                slice_del(z);
                r_stem_suffix_chain_before_ki(z);
            }
        }
        else {
            r_stem_suffix_chain_before_ki(z);
        }
        return 1;
    }

    return 1;
}
```