# finnish_ISO_8859_1_stem

## Location
[src/backend/snowball/libstemmer/stem_ISO_8859_1_finnish.c:654-714](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_ISO_8859_1_finnish.c#L654-L714)

## Overview
The finnish_ISO_8859_1_stem function is the main entry point for Finnish text stemming using the ISO-8859-1 character encoding, orchestrating a complete morphological analysis and reduction process.

## Definition

```c
}

extern int finnish_ISO_8859_1_stem(struct SN_env * z)
```
## Detailed Description
This function implements the complete Finnish stemming algorithm according to the Snowball stemming specification. It processes Finnish words by systematically removing various morphological elements in a specific order to arrive at the word's stem. The algorithm follows these sequential steps:

1. **Region marking**: Identifies morphological regions (R1, R2) using r_mark_regions
2. **Particle removal**: Removes particles and clitics using r_particle_etc
3. **Possessive suffix removal**: Handles possessive markers using r_possessive
4. **Case ending removal**: Removes grammatical case suffixes using r_case_ending
5. **Other ending removal**: Handles derivational and other suffixes using r_other_endings
6. **Plural handling**: Conditionally processes plural markers:
   - If flag I[2] is set: uses r_i_plural for 'i'/'j' plurals
   - Otherwise: uses r_t_plural for 't' plurals
7. **Final cleanup**: Performs final tidying operations using r_tidy

The function uses the I[2] flag to track whether certain morphological transformations have occurred, which determines the plural processing strategy. All operations preserve the original cursor position between steps using backtracking mechanisms.

## Parameters / Member Variables
- `*z`: Pointer to SN_env structure containing the word to be stemmed, cursor positions, morphological region boundaries, and algorithm state flags
## Dependencies
- Functions called/Symbols referenced:
  - [r_mark_regions](../r/r_mark_regions.md) (identifies morphological regions)
  - [r_particle_etc](../r/r_particle_etc.md) (removes particles and clitics)
  - [r_possessive](../r/r_possessive.md) (removes possessive suffixes)
  - [r_case_ending](../r/r_case_ending.md) (removes case endings)
  - [r_other_endings](../r/r_other_endings.md) (removes other morphological endings)
  - [r_i_plural](../r/r_i_plural.md) (handles 'i'/'j' plural markers)
  - [r_t_plural](../r/r_t_plural.md) (handles 't' plural markers)
  - [r_tidy](../r/r_tidy.md) (performs final cleanup)
- Called from (representative examples):
  - External stemming interfaces (library entry point)

## Notes and Other Information
- This is the main public interface for Finnish stemming with ISO-8859-1 encoding
- The algorithm follows the Snowball Finnish stemming specification precisely
- Uses backtracking (m2-m8 variables) to preserve cursor positions between operations
- The I[2] flag mechanism allows for context-sensitive plural processing
- Returns 1 on successful stemming, negative values on error
- Character encoding is specifically ISO-8859-1, supporting Finnish special characters (ä, ö)
- The function is marked 'extern' indicating it's part of the public API

## Simplified Source

```c
extern int finnish_ISO_8859_1_stem(struct SN_env * z) {
    // 1. Initialize: mark morphological regions (R1, R2)
    {
        int c1 = z->c;
        if (r_mark_regions(z) < 0) return -1;
        z->c = c1;
    }

    // Initialize flags and set up backward processing
    z->I[2] = 0;  // Clear plural processing flag
    z->lb = z->c;
    z->c = z->l;  // Start from end of word

    // 2. Remove particles and clitics (like -ko, -ka, -han, -pa)
    {
        int m_test = z->l - z->c;
        if (r_particle_etc(z) < 0) return -1;
        z->c = z->l - m_test;
    }

    // 3. Remove possessive suffixes (like -ni, -si, -nsa)
    {
        int m_test = z->l - z->c;
        if (r_possessive(z) < 0) return -1;
        z->c = z->l - m_test;
    }

    // 4. Remove case endings (grammatical cases)
    {
        int m_test = z->l - z->c;
        if (r_case_ending(z) < 0) return -1;
        z->c = z->l - m_test;
    }

    // 5. Remove other derivational and morphological endings
    {
        int m_test = z->l - z->c;
        if (r_other_endings(z) < 0) return -1;
        z->c = z->l - m_test;
    }

    // 6. Handle plural markers based on context
    if (z->I[2]) {
        // If flag is set, process i/j plurals
        int m_test = z->l - z->c;
        if (r_i_plural(z) < 0) return -1;
        z->c = z->l - m_test;
    } else {
        // Otherwise, process t plurals
        int m_test = z->l - z->c;
        if (r_t_plural(z) < 0) return -1;
        z->c = z->l - m_test;
    }

    // 7. Final cleanup and normalization
    {
        int m_test = z->l - z->c;
        if (r_tidy(z) < 0) return -1;
        z->c = z->l - m_test;
    }

    // Reset cursor to start and return success
    z->c = z->lb;
    return 1;
}
```