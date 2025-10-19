# basque_ISO_8859_1_stem

## Location
[src/backend/snowball/libstemmer/stem_ISO_8859_1_basque.c:1140-1178](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_ISO_8859_1_basque.c#L1140-L1178)

## Overview
The main stemming function for Basque language text using ISO-8859-1 character encoding that orchestrates the complete stemming process.

## Definition
extern int basque_ISO_8859_1_stem(struct SN_env * z)

## Detailed Description
This is the primary entry point for Basque stemming in PostgreSQL's Snowball stemmer implementation. The function coordinates the complete stemming process by first marking morphological regions (R1, R2, RV), then systematically applying suffix removal rules in a specific order: verbs (aditzak), nouns (izenak), and finally adjectives (adjetiboak). The function uses a backward processing approach, working from the end of the word toward the beginning, which is typical for agglutinative languages like Basque.

## Parameters / Member Variables
- z: Pointer to SN_env structure containing the stemming environment, including the word to be stemmed, cursor positions, and morphological region boundaries

## Dependencies
- Functions called/Symbols referenced:
  - [r_mark_regions](../r/r_mark_regions.md) (establishes morphological boundaries)
  - [r_aditzak](../r/r_aditzak.md) (processes verb suffixes)
  - [r_izenak](../r/r_izenak.md) (processes noun suffixes)
  - [r_adjetiboak](../r/r_adjetiboak.md) (processes adjective suffixes)
- Called from (representative examples):
  - External callers using the Basque ISO-8859-1 stemmer

## Notes and Other Information
This function implements the complete Basque stemming algorithm following Snowball methodology. The processing order (verbs→nouns→adjectives) reflects Basque morphological priorities. The function uses loop constructs with labels for verb and noun processing, allowing multiple suffix removals, while adjective processing is done only once. It returns 1 on successful completion of the stemming process.

## Simplified Source

```c
extern int basque_ISO_8859_1_stem(struct SN_env * z) {
    // Establish morphological region boundaries (R1, R2, RV)
    r_mark_regions(z);

    // Set up for backward processing from end of word
    z->lb = z->c;
    z->c = z->l;

    // Phase 1: Process verb suffixes (multiple passes allowed)
    while (1) {
        int saved_position = z->l - z->c;
        if (r_aditzak(z) == 0) {  // No more verb suffixes found
            z->c = z->l - saved_position;
            break;
        }
        // Continue processing if verb suffix was found and removed
    }

    // Phase 2: Process noun suffixes (multiple passes allowed)
    while (1) {
        int saved_position = z->l - z->c;
        if (r_izenak(z) == 0) {  // No more noun suffixes found
            z->c = z->l - saved_position;
            break;
        }
        // Continue processing if noun suffix was found and removed
    }

    // Phase 3: Process adjective suffixes (single pass)
    int saved_position = z->l - z->c;
    r_adjetiboak(z);  // Try to remove adjective suffix
    z->c = z->l - saved_position;  // Restore position regardless

    // Reset cursor to beginning of word
    z->c = z->lb;
    return 1;
}
```