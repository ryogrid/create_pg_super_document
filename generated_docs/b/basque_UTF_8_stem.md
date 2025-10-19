# basque_UTF_8_stem

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_basque.c:1142-1180](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_basque.c#L1142-L1180)

## Overview
The main entry point for the Basque UTF-8 stemming algorithm that orchestrates the complete stemming process by sequentially applying morphological analysis for verbs, nouns, and adjectives.

## Definition
extern int basque_UTF_8_stem(struct SN_env * z)

## Detailed Description
The basque_UTF_8_stem function implements the complete Basque stemming algorithm for UTF-8 encoded text. It follows a multi-stage approach: first marking morphological regions (R1, R2, RV), then systematically processing different word categories in order of complexity. The function starts by processing verbs (aditzak), followed by nouns (izenak), and finally adjectives (adjetiboak). Each processing stage uses backward cursor movement and attempts to match and transform appropriate suffixes. The function uses while loops with continue/break logic to handle the iterative nature of suffix removal, ensuring that multiple suffixes can be processed when applicable. This is the UTF-8 variant of the Basque Snowball stemming algorithm used in PostgreSQL's full-text search functionality.

## Parameters / Member Variables
- : Pointer to SN_env structure containing the stemming environment with UTF-8 word data, cursors, and boundaries

## Dependencies
- Functions called/Symbols referenced:
  - [r_mark_regions](../r/r_mark_regions.md) (region boundary identification)
  - [r_aditzak](../r/r_aditzak.md) (verb suffix processing)
  - [r_izenak](../r/r_izenak.md) (noun suffix processing)
  - [r_adjetiboak](../r/r_adjetiboak.md) (adjective suffix processing)
- Called from (representative examples):
  - External stemming interface (no direct references found in indexed code)

## Notes and Other Information
This is the public interface function for Basque UTF-8 stemming, marked with extern for external linkage. The processing order (verbs → nouns → adjectives) reflects the morphological complexity and priority in Basque language structure. The function returns 1 on successful completion and handles error conditions from the constituent processing functions. The cursor management ensures proper boundary handling throughout the multi-stage processing pipeline.

## Simplified Source

```c
extern int basque_UTF_8_stem(struct SN_env * z) {
    // Step 1: Mark morphological regions (R1, R2, RV)
    if (r_mark_regions(z) < 0) return -1;

    // Set up backward processing from end of word
    z->lb = z->c;
    z->c = z->l;

    // Step 2: Process verbs (aditzak) - most complex morphology
    while (1) {
        int saved_pos = z->l - z->c;
        if (r_aditzak(z) == 0) {
            z->c = z->l - saved_pos;
            break;
        }
        if (r_aditzak(z) < 0) return -1;
    }

    // Step 3: Process nouns (izenak) - medium complexity
    while (1) {
        int saved_pos = z->l - z->c;
        if (r_izenak(z) == 0) {
            z->c = z->l - saved_pos;
            break;
        }
        if (r_izenak(z) < 0) return -1;
    }

    // Step 4: Process adjectives (adjetiboak) - simplest morphology
    int saved_pos = z->l - z->c;
    r_adjetiboak(z); // Error handling optional for adjectives
    z->c = z->l - saved_pos;

    // Restore cursor to beginning
    z->c = z->lb;
    return 1; // Success
}
```