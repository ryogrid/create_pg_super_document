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