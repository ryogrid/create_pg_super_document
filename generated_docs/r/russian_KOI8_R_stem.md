# russian_KOI8_R_stem

## Location
[src/backend/snowball/libstemmer/stem_KOI8_R_russian.c:569-678](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_KOI8_R_russian.c#L569-L678)

## Overview
The main stemming function for Russian text using KOI8-R character encoding that reduces Russian words to their base forms by removing inflectional endings and suffixes.

## Definition
extern int russian_KOI8_R_stem(struct SN_env * z)

## Detailed Description
This function implements the complete Russian stemming algorithm for text encoded in KOI8-R format. The stemmer works by first normalizing the input (replacing 0xA3 characters with 'e'), marking morphological regions in the word, then systematically removing various types of endings in a specific order of priority. The algorithm follows the Porter stemming approach adapted for Russian morphology, processing perfective gerunds, reflexive forms, adjectival endings, verbs, and nouns. After removing inflectional suffixes, it optionally processes derivational suffixes and performs final cleanup operations.

The function operates on the input string in reverse (from end to beginning) to efficiently identify and remove suffixes. It uses a sophisticated priority system where certain ending types (like perfective gerunds) take precedence over others, and includes safeguards to prevent over-stemming by respecting morphological boundaries.

## Parameters / Member Variables
- : Pointer to SN_env structure containing the word to be stemmed, cursor positions, and working buffers for the stemming process

## Dependencies
- Functions called/Symbols referenced:
  - [slice_from_s](../s/slice_from_s.md) (character replacement)
  - [r_mark_regions](r_mark_regions.md) (region boundary identification)
  - [r_perfective_gerund](r_perfective_gerund.md) (perfective gerund suffix removal)
  - [r_reflexive](r_reflexive.md) (reflexive suffix removal)  
  - [r_adjectival](r_adjectival.md) (adjectival ending removal)
  - [r_verb](r_verb.md) (verbal ending removal)
  - [r_noun](r_noun.md) (nominal ending removal)
  - [slice_del](../s/slice_del.md) (suffix deletion)
  - [r_derivational](r_derivational.md) (derivational suffix processing)
  - [r_tidy_up](r_tidy_up.md) (final cleanup operations)
- Called from (representative examples):
  - No direct callers found in codebase

## Notes and Other Information
This function is part of PostgreSQL's full-text search capabilities, specifically for Russian language support. The KOI8-R encoding was historically important for Russian computing systems. The function returns 1 on successful completion or a negative error code on failure. The stemming algorithm preserves the original word structure while systematically removing morphological elements, making it suitable for information retrieval and text analysis applications.

## Simplified Source

```c
extern int russian_KOI8_R_stem(struct SN_env * z) {
    // 1. Normalize input: Replace 0xA3 characters with 'e'
    int start_pos = z->c;
    while (1) {
        int current_pos = z->c;
        while (1) {
            int char_pos = z->c;
            z->bra = z->c;
            if (z->c == z->l || z->p[z->c] != 0xA3) {
                // No more 0xA3 characters, move to next position
                z->c = char_pos;
                if (z->c >= z->l) break; // End of string
                z->c++;
                continue;
            }
            // Found 0xA3, replace with 'e'
            z->c++;
            z->ket = z->c;
            z->c = char_pos;
            slice_from_s(z, 1, s_0); // Replace with 'e'
            break;
        }
        if (z->c >= z->l) break; // No more characters
    }
    z->c = start_pos;

    // 2. Mark morphological regions (RV, R1, R2)
    r_mark_regions(z);

    // 3. Set up for suffix removal (work backwards)
    z->lb = z->c;
    z->c = z->l;

    // 4. Remove suffixes in priority order (within RV region)
    if (z->c >= z->I[1]) { // Check RV boundary
        int saved_lb = z->lb;
        z->lb = z->I[1];

        int saved_pos = z->l - z->c;

        // Try perfective gerund first (highest priority)
        if (r_perfective_gerund(z) == 0) {
            // If no perfective gerund, try other endings
            z->c = z->l - saved_pos;

            // Try reflexive suffix
            r_reflexive(z);

            // Try adjectival, verb, or noun endings
            int pos_before_morphology = z->l - z->c;
            if (r_adjectival(z) == 0) {
                z->c = z->l - pos_before_morphology;
                if (r_verb(z) == 0) {
                    z->c = z->l - pos_before_morphology;
                    r_noun(z);
                }
            }
        }

        // 5. Remove remaining 'и' character if present
        z->c = z->l - saved_pos;
        z->ket = z->c;
        if (z->c > z->lb && z->p[z->c - 1] == 0xC9) { // 'и' in KOI8-R
            z->c--;
            z->bra = z->c;
            slice_del(z);
        }

        // 6. Optional: Remove derivational suffixes
        r_derivational(z);

        // 7. Final cleanup
        r_tidy_up(z);

        z->lb = saved_lb;
    }

    z->c = z->lb;
    return 1;
}
```