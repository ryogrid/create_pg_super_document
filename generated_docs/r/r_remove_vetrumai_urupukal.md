# r_remove_vetrumai_urupukal

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_tamil.c:1253-1478](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_tamil.c#L1253-L1478)

## Overview
Removes Tamil case markers (vetrumai urupukal) from words as part of the Tamil stemming algorithm in PostgreSQL's Snowball stemmer implementation.

## Definition

```c
}

static int r_remove_vetrumai_urupukal(struct SN_env * z)
```
## Detailed Description
This function handles the removal of Tamil case markers (vetrumai urupukal), which are suffixes that indicate grammatical case relationships in Tamil morphology. The function implements a sophisticated multi-stage approach:

1. **Initial Setup**: Sets working variables and validates minimum word length
2. **Primary Pattern Matching**: Attempts to match specific case marker patterns and either deletes them or replaces them with standardized forms
3. **Secondary Matching**: Handles additional case marker patterns with different replacement strategies
4. **Conditional Processing**: Applies context-sensitive rules based on preceding character patterns
5. **Post-processing**: Performs final character corrections and applies ending fixes

The function uses multiple arrays (a_18, a_19, a_20, a_21) for pattern matching and employs various string constants (s_71 through s_103) representing Tamil case markers and their standardized replacements.

## Parameters / Member Variables
- `*z`: Pointer to the Snowball environment structure () containing:
## Dependencies
- Functions called/Symbols referenced:
  - : Validates minimum word length before processing
  - : Performs backward string equality checking for suffix patterns
  - : Searches for patterns in predefined suffix arrays
  - : Checks UTF-8 string length for conditional processing
  - : Deletes matched text segment
  - : Replaces matched text with specified string
  - : Applies post-processing character corrections

- Called from (representative examples):
  - : Main Tamil stemming function

## Notes and Other Information
- Returns 1 on successful processing, 0 or negative values on failure
- Sets both  and  flags when modifications are made
- Handles complex Tamil case marker morphology including locative, accusative, dative, and other grammatical cases
- Uses conditional logic to avoid over-stemming by checking character contexts
- The name "vetrumai urupukal" translates to "case markers" in Tamil linguistics
- Implements sophisticated pattern matching to handle irregular case marker formations
- Part of a larger Tamil morphological analysis system designed for text search and indexing

## Simplified Source

```c
static int r_remove_vetrumai_urupukal(struct SN_env * z) {
    // Initialize state flags
    z->I[1] = 0;
    z->I[0] = 0;

    // Check minimum word length before processing
    int ret = r_has_min_length(z);
    if (ret <= 0) return ret;

    // Set up backward processing boundaries
    z->lb = z->c;
    z->c = z->l;

    int saved_position = z->l - z->c;

    // Stage 1: Try simple 6-character deletion pattern
    int test_position = z->l - z->c;
    z->ket = z->c;
    if (eq_s_b(z, 6, s_71)) {
        z->bra = z->c;
        slice_del(z);
        z->c = z->l - test_position;
        goto success;
    }

    // Stage 2: Try complex patterns with context validation
    z->c = z->l - saved_position;
    test_position = z->l - z->c;
    z->ket = z->c;

    // Check 9-character or 3-character patterns with exclusion validation
    if (eq_s_b(z, 9, s_72) || eq_s_b(z, 3, s_73)) {
        // Ensure not in exclusion list
        if (!find_among_b(z, a_18, 6)) {
            z->bra = z->c;
            slice_from_s(z, 3, s_76); // Replace with standardized form
            z->c = z->l - test_position;
            goto success;
        }
    }

    // Check 3-character pattern with complex validation
    if (eq_s_b(z, 3, s_74)) {
        if (find_among_b(z, a_19, 6) && eq_s_b(z, 3, s_75)) {
            z->bra = z->c;
            slice_from_s(z, 3, s_76);
            z->c = z->l - test_position;
            goto success;
        }
    }

    // Stage 3: Try multiple 9-character patterns
    z->c = z->l - saved_position;
    test_position = z->l - z->c;
    z->ket = z->c;

    if (eq_s_b(z, 9, s_77) || eq_s_b(z, 9, s_78) || eq_s_b(z, 9, s_79) ||
        eq_s_b(z, 9, s_80) || eq_s_b(z, 15, s_83) || eq_s_b(z, 21, s_84) ||
        eq_s_b(z, 9, s_85) || eq_s_b(z, 9, s_87) || eq_s_b(z, 9, s_88) ||
        eq_s_b(z, 12, s_89) || eq_s_b(z, 9, s_91)) {

        z->bra = z->c;
        slice_from_s(z, 3, s_92); // Replace with standardized form
        z->c = z->l - test_position;
        goto success;
    }

    // Special pattern with context check
    if (eq_s_b(z, 9, s_81)) {
        if (!eq_s_b(z, 3, s_82)) {
            z->bra = z->c;
            slice_from_s(z, 3, s_92);
            z->c = z->l - test_position;
            goto success;
        }
    }

    // Length-conditional pattern
    if (len_utf8(z->p) >= 7 && eq_s_b(z, 12, s_86)) {
        z->bra = z->c;
        slice_from_s(z, 3, s_92);
        z->c = z->l - test_position;
        goto success;
    }

    // Pattern with exclusion check
    if (eq_s_b(z, 6, s_90)) {
        if (!find_among_b(z, a_20, 8)) {
            z->bra = z->c;
            slice_from_s(z, 3, s_92);
            z->c = z->l - test_position;
            goto success;
        }
    }

    // Stage 4: Try deletion patterns
    z->c = z->l - saved_position;
    test_position = z->l - z->c;
    z->ket = z->c;

    if (eq_s_b(z, 9, s_93) || eq_s_b(z, 12, s_94) || eq_s_b(z, 12, s_95) ||
        eq_s_b(z, 12, s_96) || eq_s_b(z, 12, s_97) || eq_s_b(z, 12, s_98)) {

        z->bra = z->c;
        slice_del(z); // Complete removal
        z->c = z->l - test_position;
        goto success;
    }

    // Pattern with exclusion check for deletion
    if (eq_s_b(z, 6, s_99)) {
        if (!find_among_b(z, a_21, 8)) {
            z->bra = z->c;
            slice_del(z);
            z->c = z->l - test_position;
            goto success;
        }
    }

    // Stage 5: Final fallback pattern
    z->c = z->l - saved_position;
    test_position = z->l - z->c;
    z->ket = z->c;

    if (!eq_s_b(z, 3, s_100)) {
        return 0; // No patterns matched
    }

    z->bra = z->c;
    slice_from_s(z, 3, s_101);
    z->c = z->l - test_position;

success:
    // Mark successful processing
    z->I[1] = 1;
    z->I[0] = 1;

    // Optional final pattern check
    int final_position = z->l - z->c;
    z->ket = z->c;
    if (eq_s_b(z, 9, s_102)) {
        z->bra = z->c;
        slice_from_s(z, 3, s_103);
    }
    z->c = z->l - final_position;

    z->c = z->lb;

    // Apply post-processing corrections
    r_fix_endings(z);

    return 1; // Success
}
```