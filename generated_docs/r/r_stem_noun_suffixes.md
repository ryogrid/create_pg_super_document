# r_stem_noun_suffixes

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_turkish.c:1354-1863](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_turkish.c#L1354-L1863)

## Overview
Comprehensive function for identifying and removing Turkish noun suffixes, handling complex morphological patterns including case markers, possessives, and plural forms in the Snowball stemming algorithm.

## Definition

```c
}

static int r_stem_noun_suffixes(struct SN_env * z)
```
## Detailed Description
This function is the primary component for processing noun suffixes in Turkish morphological analysis. It implements an extensive branching algorithm that handles the full spectrum of Turkish noun inflection patterns, including:

- **Plural markers** (-lAr/-ler): Standard plural suffixes
- **Case suffixes**: Locative (-ncA), ablative (-DAn), genitive (-nUn), instrumental (-ylA)
- **Locative variants**: Multiple forms (-ndA, -nA, -DA, -yU, -yA)  
- **Possessive markers**: Various person/number combinations
- **Suffix chains**: Complex combinations processed via recursive calls

The algorithm uses extensive backtracking with multiple decision points to handle Turkish's agglutinative morphology where numerous suffixes can be layered on a noun stem. Each branch processes specific suffix patterns, removes them via slice operations, and optionally continues with recursive suffix chain processing.

Key processing phases include:
1. **Primary suffix identification**: Plural, case, and locative markers
2. **Secondary suffix processing**: Possessives and remaining case forms  
3. **Chain processing**: Recursive handling of complex suffix combinations
4. **Fallback patterns**: Alternative morphological structures

## Parameters / Member Variables
- : Pointer to the Snowball environment structure containing:
  - : Current cursor position in the string
  - : Length/end position of string
  - /: Boundary markers for suffix identification and removal
  - : Character array being processed

## Dependencies
- Functions called/Symbols referenced:
  - : Identifies plural markers (-lar/-ler)
  - : Identifies plural possessive combinations
  - : Identifies locative case suffixes
  - : Identifies locative variants (-nda/-nde)
  - : Identifies locative variants (-na/-ne)
  - : Identifies ablative variants (-ndan/-nden)
  - : Identifies accusative case (-nu/-nü)
  - : Identifies ablative case (-dan/-den)
  - : Identifies genitive case (-nun/-nün)
  - : Identifies instrumental case (-yla/-yle)
  - : Identifies locative case (-da/-de)
  - : Identifies accusative variants (-yu/-yü)
  - : Identifies dative variants (-ya/-ye)
  - : Identifies possessive suffixes (various persons/numbers)
  - : Identifies 3rd person possessive markers
  - : Recursively processes suffix chains before relativizer
  - : Removes identified suffix segments

- Called from:
  - : Main Turkish stemming function

## Notes and Other Information
- Returns 1 on successful processing, 0 or negative values on failure/errors
- Uses extensive branching logic with labeled gotos for complex pattern matching
- Handles Turkish vowel harmony through subsidiary marking functions
- Manages cursor position restoration using saved positions (m1-m27 variables)
- Essential component of Turkish noun phrase analysis and normalization
- Processes suffixes in reverse order (right-to-left) following agglutinative word structure
- Integrates with suffix chain processing for handling complex morphological patterns
- Critical for Turkish text search, indexing, and natural language processing applications
- Implements comprehensive coverage of Turkish nominal morphology patterns

## Simplified Source

```c
static int r_stem_noun_suffixes(struct SN_env * z) {
    z->ket = z->c;

    // Branch 1: Handle plural suffix (-lAr)
    if (r_mark_lAr(z)) {
        z->bra = z->c;
        slice_del(z);  // Remove plural suffix

        // Try to process suffix chains after removing plural
        r_stem_suffix_chain_before_ki(z);
        return 1;
    }

    // Branch 2: Handle locative case (-ncA)
    z->ket = z->c;
    if (r_mark_ncA(z)) {
        z->bra = z->c;
        slice_del(z);

        z->ket = z->c;
        // Try possessive plural combinations or possessives/3rd person
        if (r_mark_lArI(z)) {
            z->bra = z->c;
            slice_del(z);
        }
        else if (r_mark_possessives(z) || r_mark_sU(z)) {
            z->bra = z->c;
            slice_del(z);

            z->ket = z->c;
            if (r_mark_lAr(z)) {
                z->bra = z->c;
                slice_del(z);
                r_stem_suffix_chain_before_ki(z);
            }
        }
        else if (r_mark_lAr(z)) {
            z->bra = z->c;
            slice_del(z);
            r_stem_suffix_chain_before_ki(z);
        }
        return 1;
    }

    // Branch 3: Handle other locative variants (-ndA, -nA)
    z->ket = z->c;
    if (r_mark_ndA(z) || r_mark_nA(z)) {
        // Process possessive plural or 3rd person markers
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

    // Branch 4: Handle ablative variants (-ndAn, -nU)
    z->ket = z->c;
    if (r_mark_ndAn(z) || r_mark_nU(z)) {
        if (r_mark_sU(z)) {
            z->bra = z->c;
            slice_del(z);

            z->ket = z->c;
            if (r_mark_lAr(z)) {
                z->bra = z->c;
                slice_del(z);
                r_stem_suffix_chain_before_ki(z);
            }
        }
        else if (r_mark_lArI(z)) {
            // Handle possessive plural
        }
        return 1;
    }

    // Branch 5: Handle ablative case (-DAn)
    z->ket = z->c;
    if (r_mark_DAn(z)) {
        z->bra = z->c;
        slice_del(z);

        z->ket = z->c;
        if (r_mark_possessives(z)) {
            z->bra = z->c;
            slice_del(z);

            z->ket = z->c;
            if (r_mark_lAr(z)) {
                z->bra = z->c;
                slice_del(z);
                r_stem_suffix_chain_before_ki(z);
            }
        }
        else if (r_mark_lAr(z)) {
            z->bra = z->c;
            slice_del(z);
            r_stem_suffix_chain_before_ki(z);
        }
        else {
            r_stem_suffix_chain_before_ki(z);
        }
        return 1;
    }

    // Branch 6: Handle genitive/instrumental case (-nUn, -ylA)
    z->ket = z->c;
    if (r_mark_nUn(z) || r_mark_ylA(z)) {
        z->bra = z->c;
        slice_del(z);

        z->ket = z->c;
        if (r_mark_lAr(z)) {
            z->bra = z->c;
            slice_del(z);
            r_stem_suffix_chain_before_ki(z);
        }
        else if (r_mark_possessives(z) || r_mark_sU(z)) {
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

    // Branch 7: Handle possessive plural (-lArI)
    z->ket = z->c;
    if (r_mark_lArI(z)) {
        z->bra = z->c;
        slice_del(z);
        return 1;
    }

    // Branch 8: Try suffix chain processing
    if (r_stem_suffix_chain_before_ki(z)) {
        return 1;
    }

    // Branch 9: Handle remaining case suffixes (-DA, -yU, -yA)
    z->ket = z->c;
    if (r_mark_DA(z) || r_mark_yU(z) || r_mark_yA(z)) {
        z->bra = z->c;
        slice_del(z);

        z->ket = z->c;
        if (r_mark_possessives(z)) {
            z->bra = z->c;
            slice_del(z);

            z->ket = z->c;
            if (r_mark_lAr(z)) {
                // Optional plural after possessive
            }
        }
        else if (r_mark_lAr(z)) {
            // Handle plural
        }

        z->bra = z->c;
        slice_del(z);
        z->ket = z->c;
        r_stem_suffix_chain_before_ki(z);
        return 1;
    }

    // Branch 10: Handle final possessives/3rd person
    z->ket = z->c;
    if (r_mark_possessives(z) || r_mark_sU(z)) {
        z->bra = z->c;
        slice_del(z);

        z->ket = z->c;
        if (r_mark_lAr(z)) {
            z->bra = z->c;
            slice_del(z);
            r_stem_suffix_chain_before_ki(z);
        }
    }

    return 1;
}
```